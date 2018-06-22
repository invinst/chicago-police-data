# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# merge/resolve_complaints/src/Makefile

.PHONY: all clean
# Additional files created: resolve_complaints.log, resolve_complaints.yaml,
# output/complaints-accused.csv.gz
all: \
		output/complaints-complaints.csv.gz

output/complaints-complaints.csv.gz: \
		src/resolve_complaints.py \
		input/complaints-complaints_1967-1999_2016-12.csv.gz \
    input/complaints-complaints_2000-2016_2016-11.csv.gz \
    input/complaints-complaints_2000-2018_2018-03.csv.gz \
    input/complaints-accused_1967-1999_2016-12.csv.gz \
    input/complaints-accused_2000-2016_2016-11.csv.gz \
    input/complaints-accused_2000-2018_2018-03.csv.gz
	python $<


clean:
	rm output/*


# done
