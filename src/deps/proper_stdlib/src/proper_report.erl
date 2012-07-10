-module(proper_report).
-export([report/4, report_ct/4, report/5]).
-define(NAMES,
        ["MARY", "PATRICIA", "LINDA", "BARBARA", "ELIZABETH", "JENNIFER",
         "MARIA", "SUSAN", "MARGARET","DOROTHY", "LISA", "NANCY", "KAREN",
         "BETTY", "HELEN", "SANDRA", "DONNA", "CAROL", "RUTH", "SHARON",
         "MICHELLE", "LAURA", "SARAH", "KIMBERLY", "DEBORAH", "JESSICA",
         "SHIRLEY", "CYNTHIA", "ANGELA", "MELISSA", "BRENDA", "AMY", "ANNA",
         "REBECCA","VIRGINIA", "KATHLEEN", "PAMELA", "MARTHA","DEBRA","AMANDA",
         "STEPHANIE", "CAROLYN", "CHRISTINE", "MARIE", "JANET", "CATHERINE",
         "FRANCES", "ANN", "JOYCE", "DIANE","ALICE","JULIE","HEATHER","TERESA",
         "DORIS", "GLORIA", "EVELYN", "JEAN", "CHERYL", "MILDRED", "KATHERINE",
         "JOAN", "ASHLEY", "JUDITH", "ROSE", "JANICE","KELLY","NICOLE", "JUDY",
         "CHRISTINA", "KATHY", "THERESA", "BEVERLY", "DENISE", "TAMMY", "IRENE",
         "JANE", "LORI", "RACHEL", "MARILYN", "ANDREA", "KATHRYN", "LOUISE",
         "SARA", "ANNE", "JACQUELINE","WANDA","BONNIE","JULIA", "RUBY", "LOIS",
         "TINA", "PHYLLIS", "NORMA","PAULA","DIANA","ANNIE", "LILLIAN", "EMILY",
         "ROBIN"]).

report(Cmds, History, FinalState, Result) ->
  report(Cmds, History, FinalState, Result, fun (Format, Data) -> io:format(user, Format, Data) end).
report_ct(Cmds, History, FinalState, Result) ->
  report(Cmds, History, FinalState, Result, fun (Format, Data) -> ct:pal(Format, Data) end).

report([{init, _InitialState}|Cmds], History, FinalState, Result, Fun) ->
   report(Cmds, History, FinalState, Result, Fun);
report(Cmds, History, FinalState, Result, Fun) ->
    Vars = lists:sort(find_reused(Cmds)),
    VarsArray = lists:foldl(
                  fun(X, A) -> array:set(X, array:get(X, A) + 1, A) end,
                  array:new({default, 0}), Vars),
    VarsPLst = my_enumerate(array:sparse_to_orddict(VarsArray)),
    States = [S || {S, _R} <- History] ++ [FinalState],
    Results = [init|[R || {_S, R} <- History]],
    FullHistory = lists:zip(Results, States),

    Fun("~n---Result:~n~p~n", [Result]),
    Sequence = [ translate(VarsPLst, Cmd) || Cmd <- Cmds],
    Fun("~n---Command sequence:~n~s~n", [string:join(Sequence,"\n")]),
    Fun("~n---States and results:~n", []),
    AdjustedSequence = lists:reverse(
                         lists:nthtail(length(Sequence) + 1 -
                                       length(FullHistory),
                                       lists:reverse(["init"|Sequence]))),
    [Fun("Command:~n~s~n"
         "Result:~n~s~n"
         "State was:~n~s~n"
         "--------------------~n",
         [
           Command,
           io_lib_pretty:print(Res, 1, 80, -1),
           io_lib_pretty:print(State, 1, 80, -1)
         ])
       || {{Res, State}, Command} <- lists:zip(FullHistory, 
                                               AdjustedSequence) ].

find_reused(Cmds) ->
    find_reused(Cmds, []).

find_reused({var, VarN}, Vars) ->
    [VarN|Vars];
find_reused(Tuple, Vars) when is_tuple(Tuple) ->
    find_reused(tuple_to_list(Tuple), Vars);
find_reused(Cmds, Vars) when is_list(Cmds) ->
    VarsDeep = [find_reused(Cmd, Vars) || Cmd <- Cmds],
    lists:flatten(VarsDeep);
find_reused(_, Vars) -> Vars.

translate(_VarsDict, init) ->
    "Initial state";
translate(VarsDict, {set, {var, VarN}, Call}) ->
    Prefix = case proplists:lookup(VarN, VarsDict) of
                 {_, none} -> "";
                 {_, Name} -> Name ++ " = "
             end,
    Prefix ++ translate_call(VarsDict, Call).

translate_call(VarsDict, {call, erlang, element, [N, {var, VarN}]}) ->
    case proplists:lookup(VarN, VarsDict) of
        {_, none} -> exit(var_not_exists);
        {_, Name} -> io_lib:format("~s#~B", [Name, N])
    end;
translate_call(VarsDict, {call, M, F, A}) ->
    io_lib:format("~s:~s(~s)", [M, F, translate_args(VarsDict, A)]).

translate_args(VarsDict, Args) ->
    TranslatedArgs = [translate_arg(VarsDict, Arg) || Arg <- Args],
    string:join(TranslatedArgs, ", ").

translate_arg(VarsDict, {call, _, _, _}=C) -> translate_call(VarsDict, C);
translate_arg(VarsDict, {var, VarN}) ->
    case proplists:lookup(VarN, VarsDict) of
        {_, none} -> exit(var_not_exists);
        {_, Name} -> io_lib:format("~s#~B", [Name, VarN])
    end;
translate_arg(VarsDict, X) when is_list(X) ->
    PL = io_lib:printable_list(X),
    PUL = io_lib:printable_unicode_list(X),
    if
      X == [] -> "[]";
      PL -> lists:flatten(io_lib:format("~p",[X]));
      PUL -> lists:flatten(io_lib:format("~p",[X]));
      true -> "[" ++ translate_args(VarsDict, X) ++ "]"
    end;
translate_arg(VarsDict, X) when is_tuple(X) ->
    "{" ++ translate_args(VarsDict, tuple_to_list(X)) ++ "}";
translate_arg(_VarsDict, X) -> io_lib:format("~p", [X]).

my_enumerate(Lst) ->
    lists:reverse(my_enumerate(Lst, ?NAMES, [])).

my_enumerate([], _, Res) -> Res;
my_enumerate([{Var, Num}|Rest], [Name|Names], Res) when Num > 1 ->
    my_enumerate(Rest, Names, [{Var, Name}|Res]);
my_enumerate([{Var, _Num}|Rest], Names, Res) ->
    my_enumerate(Rest, Names, [{Var, none}|Res]).
