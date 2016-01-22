<?php
namespace Vitess\Grpc;

class StreamResponse
{

    private $call;

    private $iterator;

    public function __construct($call)
    {
        $this->call = $call;
        $this->iterator = $call->responses();

        if (! $this->iterator->valid()) {
            // No responses were returned. Check for error.
            Client::checkError($this->call->getStatus());
        }
    }

    public function next()
    {
        if ($this->iterator->valid()) {
            $value = $this->iterator->current();
            $this->iterator->next();
            return $value;
        } else {
            // Now that the responses are done, check for gRPC status.
            Client::checkError($this->call->getStatus());
            // If no exception is raised, indicate successful end of stream.
            return FALSE;
        }
    }

    public function close()
    {
        $this->call->cancel();
    }
}
