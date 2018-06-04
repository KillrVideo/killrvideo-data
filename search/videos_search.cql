// Create search index using CQL commands
CREATE SEARCH INDEX IF NOT EXISTS on videos;

// Create copy field to allow search on 'name' and 'tags' columns in a single query
ALTER SEARCH INDEX SCHEMA on videos SET field[@name='tags']@termVectors = 'true';
ALTER SEARCH INDEX SCHEMA on videos SET field[@name='name']@termVectors = 'true';

ALTER SEARCH INDEX SCHEMA on videos ADD types.fieldType[@class='solr.TextField', @name='textSuggest'] WITH $${ "analyzer": [ { "tokenizer" : { "class":"solr.StandardTokenizerFactory" }, "filter" : { "class":"solr.LowerCaseFilterFactory" } } ] } $$;

ALTER SEARCH INDEX SCHEMA on videos ADD fields.field[@name='search_suggestions', @type='textSuggest', @multiValued = 'false', @indexed='true'];

ALTER SEARCH INDEX SCHEMA on videos ADD copyField[@source='name',@dest='search_suggestions'];
ALTER SEARCH INDEX SCHEMA on videos ADD copyField[@source='tags',@dest='search_suggestions'];

// Suggester
ALTER SEARCH INDEX CONFIG ON videos ADD searchComponent[@name='suggest', @class='solr.SuggestComponent'] WITH  $$ {"suggester":[{"name":"searchSuggester"}, {"lookupImpl":"FSTLookupFactory"}, {"dictionaryImpl":"HighFrequencyDictionaryFactory"}, {"threshold":0.1}, {"field":"search_suggestions"} ] } $$;

ALTER SEARCH INDEX CONFIG ON videos ADD requestHandler[@name='/suggest', @class='solr.SearchHandler'] WITH  $$ {"defaults":[{"suggest":"true"}, {"suggest.count":"5"}, {"suggest.dictionary":"searchSuggester"}],"components":["suggest"]} $$;

// More Like This
ALTER SEARCH INDEX CONFIG ON videos ADD requestHandler[@name='/mlt', @class='solr.MoreLikeThisHandler'];

// Support DSE 6 searches
ALTER SEARCH INDEX SCHEMA ON killrvideo.videos 
   ADD types.fieldType [ 
      @name='TextField_intl' , 
      @class='org.apache.solr.schema.TextField' ]
   WITH  $${
      "analyzer": [ {
         "type": "index",
         "tokenizer": { "class": "solr.StandardTokenizerFactory" },
         "filter": [ { "class": "solr.LowerCaseFilterFactory" },
              { "class": "solr.ASCIIFoldingFilterFactory" }
            ]
          },
          {
            "type": "search",
            "tokenizer": { "class": "solr.StandardTokenizerFactory" },
            "filter": [
              { "class": "solr.LowerCaseFilterFactory" },
              { "class": "solr.ASCIIFoldingFilterFactory" }
            ]
          }
        ]
      }$$;

ALTER SEARCH INDEX SCHEMA ON killrvideo.videos 
  SET field[@name='name']@type = 'TextField_intl';
ALTER SEARCH INDEX SCHEMA ON killrvideo.videos 
  SET field[@name='tags']@type = 'TextField_intl';
ALTER SEARCH INDEX SCHEMA ON killrvideo.videos 
  SET field[@name='description']@type = 'TextField_intl';

// Rebuild index
RELOAD SEARCH INDEX ON videos;
REBUILD SEARCH INDEX on videos;


