# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# merge/unit-history/daily-panel/src/Makefile

.PHONY: all clean
# Additional files created: daily-panel.log, daily-panel.yaml

all: \
		output/daily-unit-panel.csv.gz

output/daily-unit-panel.csv.gz: \
		src/daily-panel.py \
                input/unit-history.csv.gz
	python $<


clean:
	rm output/*


# done
