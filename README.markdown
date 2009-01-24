tora
====

tora is an Erlang client for [Tokyo Tyrant](http://tokyocabinet.sourceforge.net/tyrantdoc/). tora speaks Tokyo Tyrant's [TCP/IP protocol](http://tokyocabinet.sourceforge.net/tyrantdoc/#protocol). This allows for more API calls than what's possible by using the memcached protocol or HTTP which Tokyo Tyrant also supports. 

### Note ###

* If all you want to do is put & get, then you probably don't need this. Use a [memcache client](http://github.com/joewilliams/merle/) or just use HTTP.
* If you want to directly use Tokyo Cabinet w/o Tokyo Tyrant, then you want [tcerl](http://code.google.com/p/tcerl/).

### INSTALL ###

    $make
    $make test (you need eunit to run the tests)
    $make docs 

### Not ready for use yet ###