<?php
namespace Vitess;

/**
 * Context is an immutable object carrying request-specific deadlines and
 * credentials (callerId).
 *
 * <p>Example usage:
 * <pre>
 * $ctx = Context::getDefault()->withDeadlineAfter(0.5); // 500ms timeout
 * $conn->doSomething($ctx, ...);
 * </pre>
 */
class Context
{

    private $deadline;

    private $callerId;

    private function __construct($deadline, $callerId)
    {
        $this->deadline = $deadline;
        $this->callerId = $callerId;
    }

    public static function getDefault()
    {
        return new Context(NULL, NULL);
    }

    /**
     * withDeadline returns a new Context with a sooner deadline.
     *
     * @param float $deadline
     *            UNIX timestamp, as from microtime(TRUE).
     */
    public function withDeadline($deadline)
    {
        if (isset($this->deadline) && $this->deadline > $deadline) {
            return new Context($deadline, $this->callerId);
        }
        return $this;
    }

    /**
     * withDeadlineAfter returns a new Context with a deadline based on the
     * current time.
     *
     * @param float $seconds
     *            Number of seconds to add to current time.
     */
    public function withDeadlineAfter($seconds)
    {
        return $this->withDeadline(microtime(TRUE) + $seconds);
    }

    /**
     * withCallerId returns a new Context with the given callerId.
     */
    public function withCallerId($callerId)
    {
        return new Context($this->deadline, $callerId);
    }

    public function getDeadline()
    {
        return $this->deadline;
    }

    public function getCallerId()
    {
        return $this->callerId;
    }

    public function isCancelled()
    {
        if (isset($this->deadline)) {
            return microtime(TRUE) > $this->deadline;
        }
        return FALSE;
    }
}