<?php

class VTException extends Exception {
}

class VTBadInputError extends VTException {
}

class VTIntegrityError extends VTException {
}

class VTTransientError extends VTException {
}

class VTDeadlineExceededError extends VTTransientError {
}

class VTUnauthenticatedError extends VTException {
}

