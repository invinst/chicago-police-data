# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# merge/unit-history/yearly-panel/src/Makefile

.PHONY: all clean
# Additional files created: yearly-panel.log, yearly-panel.yaml

all: \
		output/yearly-unit-panel.csv.gz

output/yearly-unit-panel.csv.gz: \
		src/yearly-panel.py \
                input/unit-history.csv.gz
	python $<


clean:
	rm output/*


# done
