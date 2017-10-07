# Wikidata to json

Extract everything from wikidata dump into json files

# run

(using Docker)

```
docker build --tag wikidata2json .
docker run --rm -ti -v `pwd`:/opt/code wikidata2json
```
