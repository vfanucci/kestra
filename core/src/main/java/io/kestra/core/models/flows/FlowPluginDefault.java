package io.kestra.core.models.flows;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

import io.kestra.core.validations.PluginDefaultValidation;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * A plugin default entry scoped to a single flow.
 * <p>
 * For type-matched defaults (no {@code ref}), the {@code forced} flag is ignored: flow-level defaults
 * cannot override values enforced at namespace or tenant level by administrators (see
 * {@code PluginDefaultService#getFlowDefaults}). For named ({@code ref}) bundles, {@code forced} is
 * honored so a flow can declare a bundle that overrides task-level properties when referenced.
 */
@Getter
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@PluginDefaultValidation
public class FlowPluginDefault implements PluginDefaultSpec {
    @NotNull
    private String type;

    @Builder.Default
    private boolean forced = false;

    @Schema(
        title = "Optional reference id used to apply this default only to plugins that opt in via `pluginDefaultsRef`."
    )
    private String ref;

    @Schema(
        type = "object",
        additionalProperties = Schema.AdditionalPropertiesValue.FALSE
    )
    private Map<String, Object> values;
}
