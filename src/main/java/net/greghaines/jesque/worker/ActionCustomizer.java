package net.greghaines.jesque.worker;

/**
 * A callback that enables the application of customizations to newly-instantiated actions
 *
 * @author Noam Y. Tenne
 */
public interface ActionCustomizer
{
    /**
     * Called after the construction of an action and prior to its execution
     *
     * @param actionInstance To-be executed action instance
     */
    void customize(Object actionInstance);
}
