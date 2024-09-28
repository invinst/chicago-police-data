# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/awards_1967-2017_2017-08_p061715/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_awards_1967-2017_2017-08.csv.gz

all: \
		output/awards_1967-2017_2017-08.csv.gz

output/awards_1967-2017_2017-08.csv.gz: \
		src/import.py \
                input/Awards_Data_(New_Copy).csv
	python $<


clean:
	rm output/*


# done
