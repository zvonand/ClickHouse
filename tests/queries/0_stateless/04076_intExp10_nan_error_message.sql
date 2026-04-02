SELECT intExp10(nan); -- { serverError BAD_ARGUMENTS, message_re "intExp10 must not be called with nan" }
