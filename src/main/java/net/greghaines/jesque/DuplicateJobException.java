package net.greghaines.jesque;

/**
 * Created by dimav
 * on 14/09/17 22:23.
 */
public class DuplicateJobException extends RuntimeException{

    public DuplicateJobException(final String message) {

        super(message);
    }
}
