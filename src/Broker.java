/**
 * A Broker is a named communication endpoint responsible for
 * establishing channels between tasks.
 *
 * Concrete subclasses define how connections are implemented.
 * The first implementation will be LocalBroker, where all brokers
 * and channels exist inside the same JVM.
 */
public abstract class Broker {

    // Unique name of this broker
    private final String name;

    /**
     * Creates a broker with the specified name.
     *
     * @param name unique, non-empty name of the broker
     * @throws IllegalArgumentException if the name is null,
     *                                  empty or contains only whitespace
     */
    protected Broker(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "Broker name must not be null or empty"
            );
        }

        this.name = name;
    }

    /**
     * Returns this broker's name.
     *
     * @return the broker name
     */
    public final String getName() {
        return name;
    }

    /**
     * Waits for an incoming connection on the specified port.
     *
     * This operation blocks until another broker connects to this
     * broker on the same port.
     *
     * Only one accept operation may wait on a particular port at
     * a given time.
     *
     * @param port local port on which the connection is accepted
     * @return the local endpoint of the newly established channel
     *
     * @throws IllegalArgumentException if the port is invalid
     * @throws IllegalStateException if another accept operation
     *                               is already waiting on this port
     */
    public abstract Channel accept(int port);

    /**
     * Connects to another broker.
     *
     * This operation blocks until the destination broker accepts
     * the connection on the specified port.
     *
     * @param name name of the destination broker
     * @param port destination port
     * @return the local endpoint of the newly established channel
     *
     * @throws IllegalArgumentException if the broker name is invalid,
     *                                  the port is invalid, or the
     *                                  destination broker does not exist
     */
    public abstract Channel connect(String name, int port);

    /**
     * Checks whether a port number is valid.
     *
     * Valid ports are between 0 and 65535, inclusive.
     *
     * @param port port number to validate
     * @throws IllegalArgumentException if the port is invalid
     */
    protected static void checkPort(int port) {
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException(
                "Invalid port number: " + port
            );
        }
    }

    /**
     * Checks whether a broker name is valid.
     *
     * @param name broker name to validate
     * @throws IllegalArgumentException if the name is null,
     *                                  empty or contains only whitespace
     */
    protected static void checkBrokerName(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "Broker name must not be null or empty"
            );
        }
    }

    /**
     * Returns a readable representation of this broker.
     *
     * @return broker description
     */
    @Override
    public String toString() {
        return "Broker{name='" + name + "'}";
    }
}