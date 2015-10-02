Recommended usage of compstats.sh:
```
./compstats.sh | ../bin/csv2kettle.py -t int -c 'hostname:string'
or dockerized:
docker run -t -i --rm IMAGE_ID bash -c "/pykettle/samples/compstats.sh | /pykettle/bin/csv2kettle.py -t int -c 'hostname:string' -a 172.17.42.1"
```

