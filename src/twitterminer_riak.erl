-module(twitterminer_riak).

-export([twitter_example/0, twitter_save_pipeline/3, get_riak_hostport/1, start/0,starting/0]).

-record(hostport, {host, port}).

%in order to start it headless
start() ->
  spawn(?MODULE, starting, []).

%this is just in order to start everyting the the same thread
starting() ->
  {ok, StartList} = application:ensure_all_started(twitterminer),
  io:format("Has checked... ~p~n", [StartList]),
  twitter_example().



% This file contains example code that connects to Twitter and saves tweets to Riak.
% It would benefit from refactoring it together with twitterminer_source.erl.

keyfind(Key, L) ->
  {Key, V} = lists:keyfind(Key, 1, L),
  V.

%% @doc Get Twitter account keys from a configuration file.
get_riak_hostport(Name) ->
  {ok, Nodes} = application:get_env(twitterminer, riak_nodes),
  {Name, Keys} = lists:keyfind(Name, 1, Nodes),
  #hostport{host=keyfind(host, Keys),
            port=keyfind(port, Keys)}.

%% @doc This example will download a sample of tweets and print it.
twitter_example() ->
  io:format("Starting...~n"),

  URL = "https://stream.twitter.com/1.1/statuses/sample.json",

  % We get our keys from the twitterminer.config configuration file.
  Keys = twitterminer_source:get_account_keys(account1),

  RHP = get_riak_hostport(riak1),
  {ok, R} = riakc_pb_socket:start(RHP#hostport.host, RHP#hostport.port),

  % Run our pipeline
  P = twitterminer_pipeline:build_link(twitter_save_pipeline(R, URL, Keys)),

  % If the pipeline does not terminate after 60 s, this process will
  % force it.
  T = spawn_link(fun () ->
        receive
          cancel -> ok
        after 6000000 -> % Sleep fo 6000 s
            twitterminer_pipeline:terminate(P)
        end
    end),

  Res = twitterminer_pipeline:join(P),
  T ! cancel,
  Res.

%% @doc Create a pipeline that connects to twitter and
%% saves tweets to Riak. We save all messages that have ids,
%% which might include delete notifications etc.
twitter_save_pipeline(R, URL, Keys) ->

  Prod = twitterminer_source:twitter_producer(URL, Keys),

  % Pipelines are constructed 'backwards' - consumer is first, producer is last.
  [
    twitterminer_pipeline:consumer(
      fun(Msg, N) -> save_tweet(R, Msg), N+1 end, 0),
    twitterminer_pipeline:map(
      fun twitterminer_source:decorate_with_id/1),
    twitterminer_source:split_transformer(),
    Prod].

%% We save only objects with several hashtags
save_tweet(R, {parsed_tweet, B, {id, _I}}) ->
  sec_idx(B, B, R);

save_tweet(_, _) -> ok.

%% Each hashtag in the list is stored under a unique key, and is then indexed under it's own name
%% so that we can search for it, under value we store it and it's hashtag "friends"
%% later allowing us to measure how often certain hashtags are used together in a tweet
sec_idx([],_Hashtag_list, _R) -> ok;
sec_idx([H | T], Hashtag_list, R) ->
io:format("Saving tweet: ~n~p",[H]),
Obj = riakc_obj:new(<<"hashtags">>, undefined, Hashtag_list), %%changed
  MD1 = riakc_obj:get_update_metadata(Obj),
  MD2 = riakc_obj:set_secondary_index(
    MD1,
    [{{binary_index, "ht"}, [H]}]),
Obj2 = riakc_obj:update_metadata(Obj, MD2),
riakc_pb_socket:put(R, Obj2),
sec_idx(T, Hashtag_list, R).
