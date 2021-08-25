
erl -pa ebin/ -pa deps/*/ebin -name mria1@127.0.0.1 -setcookie cookie -config data/app.etcd -s mria -s mria autocluster

erl -pa ebin/ -pa deps/*/ebin -name mria2@127.0.0.1 -setcookie cookie -config data/app.etcd -s mria -s mria autocluster
