SentAnalysisRep
===============

The program perfoms a sentiment analysis of Twitter posts using MapReduce parallel programming model. It is based on the article of The Ohio university researchers V.N. Khuc, C. Shivade, R. Ramnath, J.Ramathan.

The program has two components - lexicon builder and sentiment analyser. It is written using MapReduce framework, so it can be run on computer clusters, grids and scaled according to the size of input data and number of machines in a cluster.

To build a sentiment lexicon we do next steps:

1) retrieve from the web the training tweets which contain positive and negative emoticons.

2) execute the normalization of tweets: convert all words to lowercase and transform all triples of identical symbols to deuces.
So the words like "Goooooddddd" and "goooodd" will be represented by one word "goodd".

3) use Twitter POS Tagger (http://www.ark.cs.cmu.edu/TweetNLP) to retieve from training tweets only nouns, adjectives, verbs, advers, interjections, abbreviations and smiles. The word combinations like "adjective-noun" and "adverb-adjective" are considered as phrases. 

4) create a matrix of cooccurrences for the words/phrases from training tweets with window size T = 6. This matrix is kept as an Hbase table.

5) using the cooccurrence matrix build the HBase table with the values of cosine similarity between the phrases in the training tweets. 
This table represents our graph of phrases. Its structure is such:

  key    |   column family "weight"
  
  word_1 |   word_1:cos_sim_11 ... word_n:cos_sim_n1
  
  ..........................................................................
  
  word_n  |  word_n:cos_sim_n1 ... word_n:cos_sim_nn
  
As you can see, the vertices of the graph are the phrases from the training set, the edges are the values of the "weight",
i.e. cosine similarity between phrases.

6) remove edges with low weights.

7) execute the algorithm of sentiment propagation which propagates sentiment scores from seed nodes (which are positive and negative emoticons) to other nodes which are located at the distance D from them. D should not be too big, 4 is enough for it.

On the stage of sentiment analysis the scores of all words in the tweet are summed according to the built sentiment lexicon, and if the result is close to zero the tweet is considered to be neutral; if it is more than zero, then the tweet is considered to have a positive sentiment; if the tweet is less than zero, then the tweet is negative. 

For better understanding the concepts of MapReduce programming I recommend you the book "Hadoop: The  definitive guide" of Tom White.

The description of the basic algorithm of score propagation which was reimplemented by Khuc and others for MapReduce usage, and other important thoughts concerning the topic can be found in the article  of Velikovich, Blair-Goldensohn, Hannan, McDonald "The viability of web-derived polarity lexicons".
