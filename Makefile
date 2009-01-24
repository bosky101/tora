all: compile

compile: clean
	erlc -o ebin/ src/*.erl

test: clean
	erlc -DTEST -I test/ -o ebin/ src/tora.erl
	erl -pa ebin/ -noshell -s tora test -s init stop

clean:
	rm -rfv ebin/*.beam

ttstart:
	#ttserver -dmn -pid /tmp/ttserver.pid /tmp/ttserver.tcb
	ttserver /tmp/ttserver.tcb

ttstop:
	kill -TERM `cat /tmp/ttserver.pid`
