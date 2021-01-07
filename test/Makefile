all: cat_client

cat_client: cat_client.cpp
	g++ -g -std=c++11 -o $@ $< -lcatclient -pthread

.PHONY: clean

clean:
	-rm cat_client