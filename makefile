PYTHON = python3

.DEFAULT_GOAL = help

help:
	@echo "---------------HELP-----------------"
	@echo "To Start KV Store: make runkvstore"
	@echo "To Start the MapReduce process: make runmapreduce"
	@echo "------------------------------------"

runkvstore:
	${PYTHON} kv_store.py

runmapreduce:
	${PYTHON} init_cluster.py
