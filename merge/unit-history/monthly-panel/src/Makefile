# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# merge/unit-history/monthly-panel/src/Makefile

.PHONY: all clean
# Additional files created: monthly-panel.log, monthly-panel.yaml

all: \
		output/monthly-unit-panel.csv.gz

output/monthly-unit-panel.csv.gz: \
		src/monthly-panel.py \
                input/unit-history.csv.gz
	python $<


clean:
	rm output/*


# done
