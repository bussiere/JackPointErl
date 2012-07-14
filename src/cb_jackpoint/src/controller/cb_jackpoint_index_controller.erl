-module(cb_jackpoint_index_controller, [Req]).
-compile(export_all).
hello('GET', []) ->
    {ok, [{greeting, "Hello, world!"}]}. 