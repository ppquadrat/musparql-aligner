# LinkedMusic Queries (edited SPARQL working copy)

This working copy makes fifteen endpoint-specific examples self-contained and
contains substantive federated-query rewrites for the five Challenge 3 queries.
A live comparison found the same coarse execution outcomes for the official and
edited versions; these edits are therefore not claimed to improve executability.


# Challenge 1: Find anything you can find via the database's website.

## CQ 1

**Prompt:** nan

**Dataset:** nan

**Number of result rows:** nan

```sparql

```


# Challenge 1

## CQ 1

**Prompt:** Find all compositions in DIAMM that are composed by Guillaume de Machaut

**Dataset:** DIAMM

**Number of result rows:** 172

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>

SELECT ?composition
WHERE {
  GRAPH <https://linkedmusic.ca/graphs/diamm/> {
    ?composer wdt:P2888 wd:Q200580 .
    ?composition wdt:P86 ?composer .
  }
}
```

## CQ 2

**Prompt:** Find all sessions in France in The Session

**Dataset:** The Session

**Number of result rows:** 101

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>

SELECT ?session
WHERE {
  GRAPH <https://linkedmusic.ca/graphs/thesession/> {
    ?session a ts:Session ;
             wdt:P17 wd:Q142 .
  }
}
```

## CQ 3

**Prompt:** Find all MusicBrainz recordings made by Taylor Swift

**Dataset:** MusicBrainz

**Number of result rows:** 2544

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>

SELECT ?recording
WHERE {
  GRAPH <https://linkedmusic.ca/graphs/musicbrainz/>{
     ?artist a mb:Artist .
     ?artist wdt:P2888 wd:Q26876 .
     ?recording a mb:Recording .
     ?recording wdt:P175 ?artist .
    }
}
```

## CQ 4

**Prompt:** Find all Ethiopian songs in The Global Jukebox

**Dataset:** The Global Jukebox

**Number of result rows:** 114

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>

SELECT ?song ?songLabel
WHERE {
  GRAPH <https://linkedmusic.ca/graphs/theglobaljukebox/> {
    ?song rdf:type gj:Song .
    ?song wdt:P495 wd:Q115 .
    OPTIONAL { ?song rdfs:label ?songLabel . }
  }
}
```

## CQ 5

**Prompt:** Find all solos in Dig That Lick

**Dataset:** Dig That Lick

**Number of result rows:** 1685

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>

SELECT ?solo
WHERE {
  GRAPH <https://linkedmusic.ca/graphs/dig-that-lick/> {
     ?solo rdf:type dtl:Solo
  }
}
```


# Challenge 2: Find anything you can find beyond what you can find on the website because you have full access to the database.

## CQ 1

**Prompt:** nan

**Dataset:** nan

**Number of result rows:** nan

```sparql

```


# Challenge 2

## CQ 1

**Prompt:** Find all DIAMM archives and sort them by the number of sources that they contain

**Dataset:** DIAMM

**Number of result rows:** 681

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>

SELECT ?archive (COUNT(?source) AS ?sourceCount)
WHERE {
    ?archive a diamm:Archive  .
    OPTIONAL {
        ?source a diamm:Source ;
                       wdt:P276 ?archive  .
    }
}
GROUP BY ?archive
ORDER BY DESC(?sourceCount)
```

## CQ 2

**Prompt:** Find all the different time signatures for jigs in The Session

**Dataset:** The Session

**Number of result rows:** 2

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>

SELECT DISTINCT ?timeSignature
WHERE {
  ?tune a ts:Tune .
  ?tune wdt:P747 ?tuneSetting .
  ?tuneSetting wdt:P136 wd:Q1079270 .
  ?tuneSetting wdt:P3440 ?timeSignature .
}
ORDER BY ?timeSignature
```

## CQ 3

**Prompt:** Find all bands that share at least two members with Radiohead in MusicBrainz

**Dataset:** MusicBrainz

**Number of result rows:** 4

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>

SELECT ?band (COUNT(DISTINCT ?sharedMember) AS ?sharedMemberCount)
WHERE {
  ?radiohead a mb:Artist  ;
                      wdt:P2888 wd:Q44190 .
  ?radiohead wdt:P527 ?radiomember .
  
  ?band a mb:Artist ;
        wdt:P527 ?radiomember ;
        wdt:P527 ?sharedMember .
  
  ?radiohead wdt:P527 ?sharedMember .
  FILTER(?band != ?radiohead)

} GROUP BY ?band ?bandLabel
HAVING (COUNT(DISTINCT ?sharedMember) >= 2)
ORDER BY DESC(?sharedMemberCount)
```

## CQ 4

**Prompt:** Find all Global Jukebox cultures that have at least one song with flute instrumentation

**Dataset:** The Global Jukebox

**Number of result rows:** 69 (but if you include labels it becomes 75)

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>

SELECT DISTINCT ?culture
WHERE {
  ?song a gj:Song .
  ?song wdt:P2596 ?culture .
  ?song wdt:P870 wd:Q11405 .
  ?culture a gj:Culture .
}
```

## CQ 5

**Prompt:** Find all tracks in Dig That Lick recorded in New York City

**Dataset:** Dig That Lick

**Number of result rows:** 263

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>

SELECT DISTINCT ?track
WHERE {
  GRAPH <https://linkedmusic.ca/graphs/dig-that-lick/> {
    ?track a dtl:Track .
    ?track wdt:P8546 wd:Q60 .
  }
}
```


# Challenge 3: Find anything you can find with the database plus using the information in Wikidata.

## CQ 1

**Prompt:** nan

**Dataset:** nan

**Number of result rows:** nan

```sparql

```


# Challenge 3


**Federated-query note:** The five Challenge 3 queries below use `SERVICE <https://query.wikidata.org/sparql>`. They have been rewritten so that LinkedMusic first computes a small local candidate set, then sends only the bound Wikidata variables to Wikidata. The most likely query to run reliably is CQ 2, because the local subquery binds exactly one country before the Wikidata call.

## CQ 1

**Prompt:** Find archives in DIAMM with an inception after 1900

**Dataset:** DIAMM

**Number of result rows:** 55

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>
SELECT DISTINCT ?archive
WHERE {
  {
    SELECT DISTINCT ?archive ?archiveWikidata
    WHERE {
      GRAPH <https://linkedmusic.ca/graphs/diamm/> {
        ?archive a diamm:Archive ;
                 wdt:P2888 ?archiveWikidata .
      }
    }
  }

  SERVICE <https://query.wikidata.org/sparql> {
    ?archiveWikidata wdt:P571 ?inceptionDate .
    FILTER (YEAR(?inceptionDate) > 1900)
  }
}
```

## CQ 2

**Prompt:** Find the capital city of the country with the most sessions

**Dataset:** The Session

**Number of result rows:** 1 ("Washington, D.C.")

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>
SELECT ?capitalCity ?capitalCityLabel
WHERE {
  {
    SELECT ?country
    WHERE {
      GRAPH <https://linkedmusic.ca/graphs/thesession/> {
        ?session a ts:Session .
        ?session wdt:P17 ?country .
      }
    }
    GROUP BY ?country
    ORDER BY DESC(COUNT(?session))
    LIMIT 1
  }

  SERVICE <https://query.wikidata.org/sparql> {
    ?country wdt:P36 ?capitalCity .
    ?capitalCity rdfs:label ?capitalCityLabel .
    FILTER (LANG(?capitalCityLabel) = "en")
  }
}
```

## CQ 3

**Prompt:** What’s the average number of record labels that female singers in MusicBrainz have signed with?

**Dataset:** MusicBrainz

**Number of result rows:** 1 (the average is 1.400773195876289)

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>
SELECT (AVG(?labelCount) AS ?averageLabelsPerSinger)
WHERE {
  {
    SELECT ?artist (COUNT(DISTINCT ?label) AS ?labelCount)
    WHERE {
      {
        SELECT DISTINCT ?artist ?artistWikidata ?label
        WHERE {
          GRAPH <https://linkedmusic.ca/graphs/musicbrainz/> {
            ?artist a mb:Artist .
            ?artist wdt:P2888 ?artistWikidata .
            ?artist wdt:P264 ?label .
            ?artist wdt:P21 wd:Q6581072 .
          }
        }
      }

      SERVICE <https://query.wikidata.org/sparql> {
        ?artistWikidata wdt:P106 wd:Q177220 .
      }
    }
    GROUP BY ?artist
  }
}
```

## CQ 4

**Prompt:** Find all Global Jukebox songs from Africa

**Dataset:** The Global Jukebox

**Number of result rows:** 1128

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>
SELECT DISTINCT ?song
WHERE {
  {
    SELECT DISTINCT ?country
    WHERE {
      GRAPH <https://linkedmusic.ca/graphs/theglobaljukebox/> {
        ?song a gj:Song .
        ?song wdt:P2596 ?culture .
        ?culture wdt:P17 ?country .
      }
    }
  }

  SERVICE <https://query.wikidata.org/sparql> {
    ?country wdt:P30 wd:Q15 .
  }

  GRAPH <https://linkedmusic.ca/graphs/theglobaljukebox/> {
    ?song a gj:Song .
    ?song wdt:P2596 ?culture .
    ?culture wdt:P17 ?country .
  }
}
```

## CQ 5

**Prompt:** Count how many solos were done by artists of each gender in Dig That Lick

**Dataset:** Dig That Lick

**Number of result rows:** 2 (1056 solos by male artist; 18 solos by female artists)

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>
SELECT ?gender (COUNT(?solo) AS ?soloCount)
WHERE {
  {
    SELECT DISTINCT ?artist
    WHERE {
      GRAPH <https://linkedmusic.ca/graphs/dig-that-lick/> {
        ?solo a dtl:Solo ;
              wdt:P175 ?artist .
      }
    }
  }

  SERVICE <https://query.wikidata.org/sparql> {
    ?artist wdt:P21 ?gender .
  }

  GRAPH <https://linkedmusic.ca/graphs/dig-that-lick/> {
    ?solo a dtl:Solo ;
          wdt:P175 ?artist .
  }
}
GROUP BY ?gender
```


# Challenge 4: Find anything across different databases and Wikidata.

## CQ 1

**Prompt:** nan

**Dataset:** nan

**Number of result rows:** nan

```sparql

```


# Challenge 4

## CQ 1

**Prompt:** Find all songs in The Global Jukebox from countries with more than four sessions in the Session 

**Dataset:** The Global Jukebox, The Session

**Number of result rows:** 839 (2417 if we consider songs from culture

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>

SELECT DISTINCT ?song
WHERE {
  GRAPH <https://linkedmusic.ca/graphs/theglobaljukebox/> {
    ?song a gj:Song ;
          wdt:P495 ?country .
  }
  GRAPH <https://linkedmusic.ca/graphs/thesession/> {
    SELECT ?country (COUNT(DISTINCT ?session) AS ?sessionCount)
    WHERE {
      ?session a ts:Session ;
               wdt:P17 ?country .
    }
    GROUP BY ?country
    HAVING (COUNT(DISTINCT ?session) > 4)
  }
}
```

## CQ 2

**Prompt:** Find all works in MusicBrainz that, according to Dig that Lick, contains a solo performed by Charlie Parker

**Dataset:** MusicBrainz, Dig That Lick

**Number of result rows:** 4

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>

SELECT DISTINCT ?work
WHERE {
  GRAPH <https://linkedmusic.ca/graphs/dig-that-lick/> {
    ?solo a dtl:Solo ;
          wdt:P175 wd:Q103767 ; 
          wdt:P361 ?track .
    ?track wdt:P2888 ?wikidataWork .
  }

  GRAPH <https://linkedmusic.ca/graphs/musicbrainz/> {
    ?work a mb:Work ;
          wdt:P2888 ?wikidataWork .
  }
}
```

## CQ 3

**Prompt:** Find all compositions or recordings with "death" in the title

**Dataset:** MusicBrainz, Dig That Lick, The Session, DIAMM, The Global Jukebox

**Number of result rows:** 85707

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>

SELECT DISTINCT ?entity ?label
WHERE {
  {
    GRAPH <https://linkedmusic.ca/graphs/musicbrainz/> {
      ?entity rdf:type mb:Work .
      ?entity rdfs:label ?label .
      FILTER (CONTAINS(LCASE(STR(?label)), "death"))
    }
  }
  UNION
  {
    GRAPH <https://linkedmusic.ca/graphs/musicbrainz/> {
      ?entity rdf:type mb:Recording .
      ?entity rdfs:label ?label .
      FILTER (CONTAINS(LCASE(STR(?label)), "death"))
    }
  }
  UNION
  {
    GRAPH <https://linkedmusic.ca/graphs/diamm/> {
      ?entity rdf:type diamm:Composition .
      ?entity rdfs:label ?label .
      FILTER (CONTAINS(LCASE(STR(?label)), "death"))
    }
  }
  UNION
  {
    GRAPH <https://linkedmusic.ca/graphs/thesession/> {
      ?entity rdf:type ts:Recording .
      ?entity rdfs:label ?label .
      FILTER (CONTAINS(LCASE(STR(?label)), "death"))
    }
  }
  UNION
  {
    GRAPH <https://linkedmusic.ca/graphs/dig-that-lick/> {
      ?entity rdf:type dtl:Track .
      ?entity rdfs:label ?label .
      FILTER (CONTAINS(LCASE(STR(?label)), "death"))
    }
  }
  UNION
  {
    GRAPH <https://linkedmusic.ca/graphs/thesession/> {
      ?entity rdf:type ts:Tune .
      ?entity rdfs:label ?label .
      FILTER (CONTAINS(LCASE(STR(?label)), "death"))
    }
  }
  UNION
  {
    GRAPH <https://linkedmusic.ca/graphs/theglobaljukebox/> {
      ?entity rdf:type gj:Song .
      ?entity rdfs:label ?label .
      FILTER (CONTAINS(LCASE(STR(?label)), "death"))
    }
  }
}
```

## CQ 4

**Prompt:** Find all musical instruments in the Global Jukebox featured in songs indigenous to Madagascar, and find recordings in MusicBrainz featuring these same instruments

**Dataset:** MusicBrainz, The Global Jukebox

**Number of result rows:** 284727

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>

SELECT DISTINCT ?wikidataInstrument ?recording WHERE {
  GRAPH <https://linkedmusic.ca/graphs/theglobaljukebox/> {
    ?song wdt:P495 wd:Q1019 .
    ?song wdt:P870 ?wikidataInstrument .
  }
  GRAPH <https://linkedmusic.ca/graphs/musicbrainz/> {
    ?recording wdt:P870 ?musicBrainzInstrument .
    ?musicBrainzInstrument wdt:P2888 ?wikidataInstrument .
  }
}
```

## CQ 5

**Prompt:** Find all music events that happened on a day where at least one South Korean music label dissolved

**Dataset:** MusicBrainz, The Session

**Number of result rows:** 277

```sparql
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd:    <http://www.wikidata.org/entity/>
PREFIX wdt:   <http://www.wikidata.org/prop/direct/>

PREFIX diamm: <https://linkedmusic.ca/graphs/diamm/>
PREFIX dtl:   <https://linkedmusic.ca/graphs/dig-that-lick/>
PREFIX ts:    <https://linkedmusic.ca/graphs/thesession/>
PREFIX gj:    <https://linkedmusic.ca/graphs/theglobaljukebox/>
PREFIX mb:    <https://linkedmusic.ca/graphs/musicbrainz/>

SELECT DISTINCT ?event WHERE {
  GRAPH <https://linkedmusic.ca/graphs/musicbrainz/> {
    ?label a mb:Label ;
           wdt:P17 ?area ;
           wdt:P576 ?dissolutionDate .
    ?area wdt:P2888 wd:Q884 .
  }

  {
    GRAPH <https://linkedmusic.ca/graphs/thesession/> {
      ?event a ts:Events ;
             wdt:P580 ?eventDate .
    }
  }
  UNION
  {
    GRAPH <https://linkedmusic.ca/graphs/musicbrainz/> {
      ?event a mb:Event ;
             wdt:P585 ?eventDate .
    }
  }

  FILTER (?eventDate = ?dissolutionDate)
}
```
