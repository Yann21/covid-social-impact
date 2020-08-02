gram,counts
#!/bin/bash

#tail -n +2 foo.csv
#echo -e "gram,counts
#$(cat bar.csv)" > bar.csv


for bar in ./*; do

if [ $(head -n 1 $bar | grep -c "gram,counts") -eq 1 ];
    then
        echo "."
    else
        echo -e "gram,counts
$(cat $bar)" > $bar
fi;
done
