package io.kestra.core.services;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.event.Level;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import io.kestra.core.exceptions.FlowProcessingException;
import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.models.Plugin;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.FlowInterface;
import io.kestra.core.models.flows.FlowWithException;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.flows.PluginDefault;
import io.kestra.core.models.flows.PluginDefaultSpec;
import io.kestra.core.plugins.PluginRegistry;
import io.kestra.core.runners.RunContextLogger;
import io.kestra.core.runners.RunContextLoggerFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.utils.Logs;
import io.kestra.core.utils.MapUtils;

import io.micronaut.core.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;

/**
 * Services for parsing flows and injecting plugin default values.
 */
@Singleton
@Slf4j
public class PluginDefaultService {
    private static final ObjectMapper NON_DEFAULT_OBJECT_MAPPER = JacksonMapper.ofYaml()
        .copy()
        .setDefaultPropertyInclusion(JsonInclude.Include.NON_DEFAULT);

    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofYaml().copy()
        .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
    private static final String PLUGIN_DEFAULTS_FIELD = "pluginDefaults";
    private static final String PLUGIN_DEFAULTS_REF_FIELD = "pluginDefaultsRef";

    private static final TypeReference<List<PluginDefault>> PLUGIN_DEFAULTS_TYPE_REF = new TypeReference<>() {
    };

    @Nullable
    protected PluginGlobalDefaultConfiguration pluginGlobalDefault;

    private final RunContextLoggerFactory runContextLoggerFactory;

    protected final PluginRegistry pluginRegistry;

    @Inject
    public PluginDefaultService(
        @Nullable PluginGlobalDefaultConfiguration pluginGlobalDefault,
        RunContextLoggerFactory runContextLoggerFactory,
        PluginRegistry pluginRegistry
    ) {
        this.pluginGlobalDefault = pluginGlobalDefault;
        this.runContextLoggerFactory = runContextLoggerFactory;
        this.pluginRegistry = pluginRegistry;
    }

    @PostConstruct
    void validateGlobalPluginDefault() {
        List<PluginDefault> mergedDefaults = new ArrayList<>();

        if (pluginGlobalDefault != null && pluginGlobalDefault.getDefaults() != null) {
            mergedDefaults.addAll(pluginGlobalDefault.getDefaults());
        }

        mergedDefaults.stream()
            .flatMap(pluginDefault -> this.validateDefault(pluginDefault).stream())
            .forEach(violation -> log.error("Invalid plugin default configuration: {}", violation));
    }

    /**
     * Gets all the defaults values for the given flow.
     *
     * @param flow the flow to extract default
     * @return list of {@code PluginDefault} ordered by most important first
     */
    protected List<PluginDefault> getAllDefaults(final String tenantId,
        final String namespace,
        final Map<String, Object> flow) {
        List<PluginDefault> defaults = new ArrayList<>();
        defaults.addAll(getFlowDefaults(flow));
        defaults.addAll(getGlobalDefaults());
        return defaults;
    }

    /**
     * Gets the flow-level defaults values.
     * <p>
     * For type-matched defaults (no {@code ref}), the {@code forced} flag is intentionally ignored at
     * flow level: only administrators can enforce plugin defaults (via namespace, tenant, or global
     * configuration). Any {@code forced: true} type-matched entry is silently treated as {@code forced: false}.
     * <p>
     * For named ({@code ref}) bundles, {@code forced} is honored: a flow can declare a referenced bundle
     * that overrides task-level properties when explicitly opted into via {@code pluginDefaultsRef}.
     *
     * @param flow the flow to extract default
     * @return list of {@code PluginDefault} ordered by most important first
     */
    protected List<PluginDefault> getFlowDefaults(final Map<String, Object> flow) {
        Object defaults = flow.get(PLUGIN_DEFAULTS_FIELD);
        if (defaults == null) {
            return List.of();
        }

        List<PluginDefault> flowDefaults = OBJECT_MAPPER.convertValue(defaults, PLUGIN_DEFAULTS_TYPE_REF);

        boolean hasForcedTypeDefault = flowDefaults.stream()
            .anyMatch(entry -> entry.isForced() && entry.getRef() == null);
        if (hasForcedTypeDefault) {
            log.warn(
                "Flow '{}' in namespace '{}' uses 'forced: true' in a type-matched pluginDefaults entry." +
                " The 'forced' flag is not supported for type-matched flow defaults and will be ignored." +
                " Remove it from the flow to suppress this warning.",
                flow.get("id"),
                flow.get("namespace")
            );
        }

        // keep 'forced' for named (ref) bundles, strip it for type-matched defaults
        return flowDefaults.stream()
            .map(entry -> entry.getRef() != null ? entry : entry.toBuilder().forced(false).build())
            .toList();
    }

    /**
     * Gets the global defaults values.
     *
     * @return list of {@code PluginDefault} ordered by most important first
     */
    protected List<PluginDefault> getGlobalDefaults() {
        List<PluginDefault> defaults = new ArrayList<>();

        if (pluginGlobalDefault != null && pluginGlobalDefault.getDefaults() != null) {
            defaults.addAll(pluginGlobalDefault.getDefaults());
        }
        return defaults;
    }

    /**
     * Parses the given abstract flow and injects all default values, returning a parsed {@link FlowWithSource}.
     *
     * <p>
     * If an exception occurs during parsing, the original flow is returned unchanged, and the exception is logged
     * for the passed {@code execution}
     * </p>
     *
     * @return a parsed {@link FlowWithSource}, or a {@link FlowWithException} if parsing fails
     */
    public FlowWithSource injectDefaults(FlowInterface flow, Execution execution) {
        try {
            return this.injectAllDefaults(flow, false);
        } catch (Exception e) {
            var logger = runContextLoggerFactory.create(execution);
            logger.emitLogs(RunContextLogger.logEntries(Execution.loggingEventFromException(e), LogEntry.of(execution)));
            return readWithoutDefaultsOrThrow(flow);
        }
    }

    /**
     * Parses the given abstract flow and injects all default values, returning a parsed {@link FlowWithSource}.
     *
     * <p>
     * If an exception occurs during parsing, the original flow is returned unchanged, and the exception is logged.
     * </p>
     *
     * @return a parsed {@link FlowWithSource}, or a {@link FlowWithException} if parsing fails
     */
    public FlowWithSource injectAllDefaults(FlowInterface flow, Logger logger) {
        try {
            return this.injectAllDefaults(flow, false);
        } catch (Exception e) {
            logger.warn(
                "Can't inject plugin defaults on tenant {}, namespace '{}', flow '{}' with errors '{}'",
                flow.getTenantId(),
                flow.getNamespace(),
                flow.getId(),
                e.getMessage(),
                e
            );
            return readWithoutDefaultsOrThrow(flow);
        }
    }

    private static FlowWithSource readWithoutDefaultsOrThrow(final FlowInterface flow) {
        if (flow instanceof FlowWithSource item) {
            return item;
        }

        if (flow instanceof Flow item) {
            return FlowWithSource.of(item, item.sourceOrGenerateIfNull());
        }

        // The block below should only be reached during testing for failure scenarios
        try {
            Flow parsed = NON_DEFAULT_OBJECT_MAPPER.readValue(flow.getSource(), Flow.class);
            return FlowWithSource.of(parsed, flow.getSource());
        } catch (JsonProcessingException e) {
            throw new KestraRuntimeException("Failed to read flow from source", e);
        }
    }

    /**
     * Parses the given abstract flow and injects all default values, returning a parsed {@link FlowWithSource}.
     *
     * <p>
     * If {@code strictParsing} is {@code true}, the parsing will fail in the following cases:
     * </p>
     * <ul>
     * <li>The source contains duplicate properties.</li>
     * <li>The source contains unknown properties.</li>
     * </ul>
     *
     * @param flow the flow to be parsed
     * @param strictParsing specifies if the source must meet strict validation requirements
     * @return a parsed {@link FlowWithSource}
     *
     * @throws FlowProcessingException if an error occurred while processing the flow
     */
    public FlowWithSource injectAllDefaults(final FlowInterface flow, final boolean strictParsing) throws FlowProcessingException {

        // Flow revisions created from older Kestra versions may not be linked to their original source.
        // In such cases, fall back to the generated source approach to enable plugin default injection.
        String source = flow.sourceOrGenerateIfNull();

        if (source == null) {
            // This should never happen
            String error = "Cannot apply plugin defaults. Cause: flow has no defined source.";
            Logs.logExecution(flow, log, Level.ERROR, error);
            throw new IllegalArgumentException(error);
        }

        try {
            return parseFlowWithAllDefaults(
                flow.getTenantId(),
                flow.getNamespace(),
                flow.getRevision(),
                flow.isDeleted(),
                source,
                false,
                strictParsing
            );
        } catch (ConstraintViolationException e) {
            throw new FlowProcessingException(e);
        } catch (JsonProcessingException e) {
            throw new FlowProcessingException(YamlParser.toConstraintViolationException(source, "Flow", e));
        }
    }

    /**
     * Parses the given abstract flow and injects default plugin versions, returning a parsed {@link FlowWithSource}.
     *
     * <p>
     * If the provided flow already represents a concrete {@link FlowWithSource}, it is returned as is.
     * <p/>
     *
     * <p>
     * If {@code safe} is set to {@code true} and the given flow cannot be parsed,
     * this method returns a {@link FlowWithException} instead of throwing an error.
     * <p/>
     *
     * @param flow the flow to be parsed
     * @param safe whether parsing errors should be handled gracefully
     * @param strictParsing determine strictness of flow source parsing
     * @return a parsed {@link FlowWithSource}, or a {@link FlowWithException} if parsing fails and {@code safe} is {@code true}
     *
     * @throws FlowProcessingException if an error occurred while processing the flow and {@code safe} is {@code false}.
     */
    public FlowWithSource injectVersionDefaults(final FlowInterface flow, final boolean safe, final boolean strictParsing) throws FlowProcessingException {
        if (flow instanceof FlowWithSource flowWithSource) {
            // shortcut - if the flow is already fully parsed return it immediately.
            return flowWithSource;
        }

        FlowWithSource result;

        try {
            String source = flow.getSource();
            if (source == null) {
                source = OBJECT_MAPPER.writeValueAsString(flow);
            }

            result = parseFlowWithAllDefaults(flow.getTenantId(), flow.getNamespace(), flow.getRevision(), flow.isDeleted(), source, true, strictParsing);
        } catch (Exception e) {
            if (safe) {
                Logs.logExecution(flow, log, Level.ERROR, "Failed to read flow.", e);
                result = FlowWithException.from(flow, e);

                // deleted is not part of the original 'source'
                result = result.toBuilder().deleted(flow.isDeleted()).build();
            } else {
                throw new FlowProcessingException(e);
            }
        }
        return result;
    }

    public FlowWithSource injectVersionDefaults(final FlowInterface flow, final boolean safe) throws FlowProcessingException {
        return injectVersionDefaults(flow, safe, false);
    }

    public Map<String, Object> injectVersionDefaults(@Nullable final String tenantId,
        final String namespace,
        final Map<String, Object> mapFlow) throws FlowProcessingException {
        return innerInjectDefault(tenantId, namespace, mapFlow, true);
    }

    /**
     * Parses and injects default into the given flow.
     *
     * @param tenantId the Tenant ID.
     * @param source the flow source.
     * @return a new {@link FlowWithSource}.
     *
     * @throws FlowProcessingException when parsing flow.
     */
    public FlowWithSource parseFlowWithAllDefaults(@Nullable final String tenantId, final String source, final boolean strict) throws FlowProcessingException {
        try {
            return parseFlowWithAllDefaults(tenantId, null, null, false, source, false, strict);
        } catch (ConstraintViolationException e) {
            throw new FlowProcessingException(e);
        } catch (JsonProcessingException e) {
            throw new FlowProcessingException(YamlParser.toConstraintViolationException(source, "Flow", e));
        }
    }

    /**
     * Parses and injects plugin default versions into the given flow.
     *
     * @param tenantId the Tenant ID.
     * @param source the flow source.
     * @param strictParsing specifies if the source must meet strict validation requirements
     * @return a new {@link FlowWithSource}.
     *
     * @throws FlowProcessingException when parsing flow.
     */
    public FlowWithSource parseFlowWithVersionDefaults(@Nullable final String tenantId, final String source, final boolean strictParsing) throws FlowProcessingException {
        try {
            return parseFlowWithAllDefaults(tenantId, null, null, false, source, true, strictParsing);
        } catch (ConstraintViolationException e) {
            throw new FlowProcessingException(e);
        } catch (JsonProcessingException e) {
            throw new FlowProcessingException(YamlParser.toConstraintViolationException(source, "Flow", e));
        }
    }

    /**
     * Parses and injects defaults into the given flow.
     *
     * @param tenant the tenant identifier.
     * @param namespace the namespace.
     * @param revision the flow revision.
     * @param source the flow source.
     * @return a new {@link FlowWithSource}.
     *
     * @throws ConstraintViolationException when parsing flow.
     */
    private FlowWithSource parseFlowWithAllDefaults(@Nullable final String tenant,
        @Nullable String namespace,
        @Nullable Integer revision,
        final boolean isDeleted,
        final String source,
        final boolean onlyVersions,
        final boolean strictParsing) throws ConstraintViolationException, JsonProcessingException {
        Map<String, Object> mapFlow = OBJECT_MAPPER.readValue(source, JacksonMapper.MAP_TYPE_REFERENCE);
        namespace = namespace == null ? (String) mapFlow.get("namespace") : namespace;
        revision = revision == null ? (Integer) mapFlow.get("revision") : revision;

        mapFlow = innerInjectDefault(tenant, namespace, mapFlow, onlyVersions);

        FlowWithSource withDefault = YamlParser.parse(mapFlow, FlowWithSource.class, strictParsing);

        // revision, tenants, and deleted are not in the 'source', so we copy them manually
        return withDefault.toBuilder()
            .tenantId(tenant)
            .revision(revision)
            .deleted(isDeleted)
            .source(source)
            .build();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> innerInjectDefault(final String tenantId, final String namespace, Map<String, Object> flowAsMap, final boolean onlyVersions) {
        List<PluginDefault> allDefaults = getAllDefaults(tenantId, namespace, flowAsMap);

        if (onlyVersions) {
            // filter only default 'version' property
            allDefaults = allDefaults.stream()
                .map(defaults ->
                {
                    Map<String, Object> filtered = defaults.getValues().entrySet()
                        .stream().filter(entry -> entry.getKey().equals("version"))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    return filtered.isEmpty() ? null : defaults.toBuilder().values(filtered).build();
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(ArrayList::new));
        }

        if (allDefaults.isEmpty()) {
            // no defaults to inject - return immediately.
            return flowAsMap;
        }

        addAliases(allDefaults);

        // split named (ref) bundles from type-matched defaults: a 'ref' bundle is applied only to plugins
        // that explicitly opt in via 'pluginDefaultsRef', never by type matching.
        Map<Boolean, List<PluginDefault>> byHasRef = allDefaults
            .stream()
            .collect(Collectors.partitioningBy(pluginDefault -> pluginDefault.getRef() != null));
        List<PluginDefault> typeDefaults = byHasRef.get(false);

        // one winner per ref (shadowing): a forced bundle always wins over a non-forced one. Among forced
        // bundles the lowest-priority (admin) level wins so an enforced bundle cannot be bypassed by a
        // higher-level forced override; among non-forced bundles the highest-priority level wins.
        // (allDefaults is ordered most-important-first: flow, namespace, global.)
        Map<String, PluginDefault> refWinners = resolveRefWinners(byHasRef.get(true));

        Map<Boolean, List<PluginDefault>> allDefaultsGroup = typeDefaults
            .stream()
            .collect(Collectors.groupingBy(PluginDefault::isForced, Collectors.toList()));

        // non-forced
        Map<String, List<PluginDefault>> defaults = pluginDefaultsToMap(allDefaultsGroup.getOrDefault(false, Collections.emptyList()));

        // forced plugin default need to be reverse, lower win
        Map<String, List<PluginDefault>> forced = pluginDefaultsToMap(Lists.reverse(allDefaultsGroup.getOrDefault(true, Collections.emptyList())));

        Object pluginDefaults = flowAsMap.get(PLUGIN_DEFAULTS_FIELD);
        if (pluginDefaults != null) {
            flowAsMap.remove(PLUGIN_DEFAULTS_FIELD);
        }

        // non-forced type-matched defaults are suppressed for plugins that opted into a named bundle
        if (!defaults.isEmpty()) {
            flowAsMap = (Map<String, Object>) recursiveDefaults(flowAsMap, defaults, false);
        }

        // forced type-matched defaults are admin enforcement: always applied, even to a plugin declaring
        // 'pluginDefaultsRef' (a WARN is logged for those, since the referenced bundle becomes ineffective)
        if (!forced.isEmpty()) {
            flowAsMap = (Map<String, Object>) recursiveDefaults(flowAsMap, forced, true);
        }

        // named (ref) bundles applied last; they target only plugins declaring 'pluginDefaultsRef'.
        // Skipped when only injecting version defaults (refs are not versions and must not be resolved/stripped here).
        if (!onlyVersions && !refWinners.isEmpty()) {
            flowAsMap = (Map<String, Object>) recursiveRefDefaults(flowAsMap, refWinners);
        }

        if (pluginDefaults != null) {
            flowAsMap.put(PLUGIN_DEFAULTS_FIELD, pluginDefaults);
        }

        return flowAsMap;

    }

    /**
     * Resolves the single winning bundle for each {@code ref}, given the list of all named defaults ordered
     * most-important-first (flow, namespace, global). A {@code forced} bundle always wins over a non-forced one.
     * Among forced bundles the lowest-priority (admin) level wins — the last forced occurrence — so an enforced
     * bundle cannot be bypassed by a higher-level forced override. Among non-forced bundles the highest-priority
     * level wins — the first occurrence.
     */
    private static Map<String, PluginDefault> resolveRefWinners(List<PluginDefault> refDefaults) {
        Map<String, List<PluginDefault>> byRef = refDefaults
            .stream()
            .collect(Collectors.groupingBy(PluginDefault::getRef, LinkedHashMap::new, Collectors.toList()));

        Map<String, PluginDefault> winners = new LinkedHashMap<>();
        byRef.forEach((ref, candidates) ->
        {
            List<PluginDefault> forced = candidates.stream().filter(PluginDefault::isForced).toList();
            winners.put(ref, forced.isEmpty() ? candidates.getFirst() : forced.getLast());
        });
        return winners;
    }

    /**
     * Validate a plugin default by comparing its properties with the getters of the plugin class.
     * <p>
     * If the plugin default type is unknown,
     * validation will be disabled as we cannot differentiate between a prefix or an unknown type.
     */
    public List<String> validateDefault(PluginDefaultSpec pluginDefault) {
        Class<? extends Plugin> classByIdentifier = getClassByIdentifier(pluginDefault);
        if (classByIdentifier == null) {
            // this can either be a prefix or a non-existing plugin, in both cases we cannot validate in detail
            return Collections.emptyList();
        }

        Set<String> pluginDefaultProperties = pluginDefault.getValues().keySet();
        List<String> pluginProperties = Stream.of(classByIdentifier.getMethods())
            .filter(method -> method.getName().startsWith("get") || method.getName().startsWith("is"))
            .map(method ->
            {
                if (method.getName().startsWith("get")) {
                    return method.getName().substring(3).toLowerCase();
                }
                return method.getName().substring(2).toLowerCase();
            })
            .toList();

        return pluginDefaultProperties.stream()
            .filter(property -> !pluginProperties.contains(property.toLowerCase()))
            .map(property -> "No property '" + property + "' exists in plugin '" + pluginDefault.getType() + "'")
            .toList();
    }

    protected Class<? extends Plugin> getClassByIdentifier(PluginDefaultSpec pluginDefault) {
        return pluginRegistry.findClassByIdentifier(pluginDefault.getType());
    }

    private Map<String, List<PluginDefault>> pluginDefaultsToMap(List<PluginDefault> pluginDefaults) {
        return pluginDefaults
            .stream()
            .collect(Collectors.groupingBy(PluginDefault::getType));
    }

    private void addAliases(List<PluginDefault> allDefaults) {
        List<PluginDefault> aliasedPluginDefault = allDefaults.stream()
            .map(pluginDefault ->
            {
                Class<? extends Plugin> classByIdentifier = getClassByIdentifier(pluginDefault);
                return classByIdentifier != null && !pluginDefault.getType().equals(classByIdentifier.getTypeName()) ? pluginDefault.toBuilder().type(classByIdentifier.getTypeName()).build()
                    : null;
            })
            .filter(Objects::nonNull)
            .toList();

        allDefaults.addAll(aliasedPluginDefault);
    }

    @VisibleForTesting
    Object recursiveDefaults(Object object, Map<String, List<PluginDefault>> defaults) {
        return recursiveDefaults(object, defaults, false);
    }

    Object recursiveDefaults(Object object, Map<String, List<PluginDefault>> defaults, boolean forcedPass) {
        if (object instanceof Map<?, ?> value) {
            value = value
                .entrySet()
                .stream()
                .map(
                    e -> new AbstractMap.SimpleEntry<>(
                        e.getKey(),
                        recursiveDefaults(e.getValue(), defaults, forcedPass)
                    )
                )
                .collect(HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll);

            if (value.containsKey("type")) {
                value = defaults(value, defaults, forcedPass);
            }

            return value;
        } else if (object instanceof Collection<?> value) {
            return value
                .stream()
                .map(r -> recursiveDefaults(r, defaults, forcedPass))
                .toList();
        } else {
            return object;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<?, ?> defaults(Map<?, ?> plugin, Map<String, List<PluginDefault>> defaults, boolean forcedPass) {
        boolean hasRef = plugin.containsKey(PLUGIN_DEFAULTS_REF_FIELD);

        // a plugin opting into a named bundle ('pluginDefaultsRef') ignores non-forced type-matched defaults;
        // forced (admin-enforced) defaults are always applied regardless, see below.
        if (hasRef && !forcedPass) {
            return plugin;
        }

        Object type = plugin.get("type");
        if (!(type instanceof String pluginType)) {
            return plugin;
        }

        List<PluginDefault> matching = defaults.entrySet()
            .stream()
            .filter(e -> e.getKey().equals(pluginType) || pluginType.startsWith(e.getKey()))
            .flatMap(e -> e.getValue().stream())
            .toList();

        if (matching.isEmpty()) {
            return plugin;
        }

        if (hasRef) {
            // forced defaults exist for a plugin that requested a named bundle: enforcement wins, the ref is bypassed
            log.warn(
                "A forced pluginDefault for type '{}' is enforced on a plugin declaring pluginDefaultsRef '{}'." +
                " The referenced bundle is ineffective for the enforced properties.",
                pluginType,
                plugin.get(PLUGIN_DEFAULTS_REF_FIELD)
            );
        }

        Map<String, Object> result = (Map<String, Object>) plugin;

        for (PluginDefault pluginDefault : matching) {
            if (pluginDefault.isForced()) {
                result = MapUtils.deepMerge(result, pluginDefault.getValues());
            } else {
                result = MapUtils.deepMerge(pluginDefault.getValues(), result);
            }
        }

        return result;
    }

    /**
     * Traverses the flow and applies named ({@code ref}) bundles to every plugin that opts in via
     * {@code pluginDefaultsRef}. Mirrors {@link #recursiveDefaults(Object, Map)} but matches on the
     * referenced id instead of the plugin type.
     */
    @VisibleForTesting
    Object recursiveRefDefaults(Object object, Map<String, PluginDefault> refDefaults) {
        if (object instanceof Map<?, ?> value) {
            value = value
                .entrySet()
                .stream()
                .map(
                    e -> new AbstractMap.SimpleEntry<>(
                        e.getKey(),
                        recursiveRefDefaults(e.getValue(), refDefaults)
                    )
                )
                .collect(HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll);

            if (value.containsKey(PLUGIN_DEFAULTS_REF_FIELD)) {
                value = refDefaults(value, refDefaults);
            }

            return value;
        } else if (object instanceof Collection<?> value) {
            return value
                .stream()
                .map(r -> recursiveRefDefaults(r, refDefaults))
                .toList();
        } else {
            return object;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<?, ?> refDefaults(Map<?, ?> plugin, Map<String, PluginDefault> refDefaults) {
        Object ref = plugin.get(PLUGIN_DEFAULTS_REF_FIELD);
        if (!(ref instanceof String refId)) {
            return plugin;
        }

        PluginDefault pluginDefault = refDefaults.get(refId);
        if (pluginDefault == null) {
            // unknown ref: keep 'pluginDefaultsRef' so a surviving key signals an unresolved reference
            // (flagged as a validation error in the editor and failed at runtime).
            log.warn("No pluginDefaults bundle found for ref '{}' referenced by plugin '{}'", refId, plugin.get("type"));
            return plugin;
        }

        Map<String, Object> result;
        if (pluginDefault.isForced()) {
            // forced bundle overrides the plugin's own values
            result = MapUtils.deepMerge((Map<String, Object>) plugin, pluginDefault.getValues());
        } else {
            // non-forced bundle yields to the plugin's explicit values
            result = MapUtils.deepMerge(pluginDefault.getValues(), (Map<String, Object>) plugin);
        }

        // ref resolved: consume the marker so a surviving 'pluginDefaultsRef' unambiguously means "unresolved"
        result.remove(PLUGIN_DEFAULTS_REF_FIELD);
        return result;
    }

    /**
     * Returns the distinct {@code pluginDefaultsRef} ids that remain unresolved in an already-defaulted flow.
     * A reference is unresolved when no matching {@code ref} bundle exists at flow, namespace or global level
     * (the marker is otherwise stripped during {@link #recursiveRefDefaults(Object, Map)}).
     */
    public Set<String> unresolvedPluginDefaultsRefs(FlowInterface flowWithDefaults) {
        Map<String, Object> flowAsMap = OBJECT_MAPPER.convertValue(flowWithDefaults, JacksonMapper.MAP_TYPE_REFERENCE);
        Set<String> refs = new LinkedHashSet<>();
        collectUnresolvedRefs(flowAsMap, refs);
        return refs;
    }

    private void collectUnresolvedRefs(Object object, Set<String> refs) {
        if (object instanceof Map<?, ?> value) {
            if (value.get(PLUGIN_DEFAULTS_REF_FIELD) instanceof String ref) {
                refs.add(ref);
            }
            value.values().forEach(v -> collectUnresolvedRefs(v, refs));
        } else if (object instanceof Collection<?> value) {
            value.forEach(v -> collectUnresolvedRefs(v, refs));
        }
    }
}
