package io.kestra.core.exceptions;

import java.io.Serial;

/**
 * Thrown when a plugin (task, trigger or task runner) references a named plugin-defaults bundle via
 * {@code pluginDefaultsRef} that does not exist at flow, namespace or global level.
 */
public class PluginDefaultsRefNotFoundException extends KestraRuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    public PluginDefaultsRefNotFoundException(String ref) {
        super("No pluginDefaults bundle found for pluginDefaultsRef '" + ref + "'.");
    }
}
