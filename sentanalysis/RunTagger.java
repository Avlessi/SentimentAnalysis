package sentanalysis;



//import cmu.arktweetnlp.RunTagger;
import cmu.arktweetnlp.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.List;

import cmu.arktweetnlp.impl.ModelSentence;
import cmu.arktweetnlp.impl.Sentence;
import cmu.arktweetnlp.impl.features.FeatureExtractor;
import cmu.arktweetnlp.impl.features.WordClusterPaths;
import cmu.arktweetnlp.io.CoNLLReader;
import cmu.arktweetnlp.io.JsonTweetReader;
import cmu.arktweetnlp.util.BasicFileIO;
import edu.stanford.nlp.util.StringUtils;


/**
* Commandline interface to run the Twitter POS tagger with a variety of possible input and output formats.
* Also does basic evaluation if given labeled input text.
*
* For basic usage of the tagger from Java, see instead Tagger.java.
*/
public class RunTagger {
Tagger tagger;

//Commandline I/O-ish options
String inputFormat = "auto";
String outputFormat = "auto";
int inputField = 1;

String inputFilename;
String outputFilename;
/** Can be either filename or resource name **/
String modelFilename = "/cmu/arktweetnlp/model.20120919";

public boolean noOutput = false;
public boolean justTokenize = false;

public static enum Decoder { GREEDY, VITERBI };
public Decoder decoder = Decoder.GREEDY;
public boolean showConfidence = true;

PrintStream outputStream;
Iterable<Sentence> inputIterable = null;

//Evaluation stuff
private static HashSet<String> _wordsInCluster;
//Only for evaluation mode (conll inputs)
int numTokensCorrect = 0;
int numTokens = 0;
int oovTokensCorrect = 0;
int oovTokens = 0;
int clusterTokensCorrect = 0;
int clusterTokens = 0;

public static void die(String message) {
//(BTO) I like "assert false" but assertions are disabled by default in java
System.err.println(message);
System.exit(-1);
}
public RunTagger() throws UnsupportedEncodingException {
//force UTF-8 here, so don't need -Dfile.encoding
this.outputStream = new PrintStream(System.out, true, "UTF-8");
}
public void detectAndSetInputFormat(String tweetData) throws IOException {
JsonTweetReader jsonTweetReader = new JsonTweetReader();
if (jsonTweetReader.isJson(tweetData)) {
System.err.println("Detected JSON input format");
inputFormat = "json";
} else {
System.err.println("Detected text input format");
inputFormat = "text";
}
}

public String runTagger(String line) throws IOException{

tagger = new Tagger();
if (!justTokenize) {
tagger.loadModel(modelFilename);	
}


JsonTweetReader jsonTweetReader = new JsonTweetReader();



long currenttime = System.currentTimeMillis();
int numtoks = 0;

String[] parts = line.split("\t");
String tweetData = parts[inputField-1];


if (inputFormat.equals("auto")) {
detectAndSetInputFormat(tweetData);
}


String text;
if (inputFormat.equals("json")) {
text = jsonTweetReader.getText(tweetData);
if (text==null) {
System.err.println("Warning, null text (JSON parse error?), using blank string instead");
text = "";
}
} else {
text = tweetData;
}

Sentence sentence = new Sentence();

sentence.tokens = Twokenize.tokenizeRawTweetText(text);
ModelSentence modelSentence = null;

if (sentence.T() > 0 && !justTokenize) {
modelSentence = new ModelSentence(sentence.T());
tagger.featureExtractor.computeFeatures(sentence, modelSentence);
goDecode(modelSentence);
}


String restokens = outputPrependedTagging(sentence, modelSentence, justTokenize, line);	

numtoks += sentence.T();
return restokens;
}

/** Runs the correct algorithm (make config option perhaps) **/
public void goDecode(ModelSentence mSent) {
if (decoder == Decoder.GREEDY) {
tagger.model.greedyDecode(mSent, showConfidence);
} else if (decoder == Decoder.VITERBI) {
//if (showConfidence) throw new RuntimeException("--confidence only works with greedy decoder right now, sorry, yes this is a lame limitation");
tagger.model.viterbiDecode(mSent);
}	
}



private void getconfusion(Sentence lSent, ModelSentence mSent, int[][] confusion) {
for (int t=0; t < mSent.T; t++) {
int trueLabel = tagger.model.labelVocab.num(lSent.labels.get(t));
int predLabel = mSent.labels[t];
if(trueLabel!=-1)
confusion[trueLabel][predLabel]++;
}


  }
public void evaluateSentenceTagging(Sentence lSent, ModelSentence mSent) {
for (int t=0; t < mSent.T; t++) {
int trueLabel = tagger.model.labelVocab.num(lSent.labels.get(t));
int predLabel = mSent.labels[t];
numTokensCorrect += (trueLabel == predLabel) ? 1 : 0;
numTokens += 1;
}
}

private String formatConfidence(double confidence) {
//too many decimal places wastes space
return String.format("%.4f", confidence);
}

/**
* assume mSent's labels hold the tagging.
*/
/**
* assume mSent's labels hold the tagging.
*
* @param lSent
* @param mSent
* @param inputLine -- assume does NOT have trailing newline. (default from java's readLine)
*/
public String outputPrependedTagging(Sentence lSent, ModelSentence mSent,
boolean suppressTags, String inputLine) {
//mSent might be null!

int T = lSent.T();
String[] tokens = new String[T];
String[] tags = new String[T];
String[] confs = new String[T];
for (int t=0; t < T; t++) {
tokens[t] = lSent.tokens.get(t);
if (!suppressTags) {
tags[t] = tagger.model.labelVocab.name(mSent.labels[t]);	
}
//if (showConfidence) {
//confs[t] = formatConfidence(mSent.confidences[t]);
//}
}

StringBuilder sb = new StringBuilder();

for(int i = 0; i < T; ++i) 
{	
	if(tags[i].equals("V") || tags[i].equals("T") ||tags[i].equals("N") || tags[i].equals("O") ||
		tags[i].equals("A") || tags[i].equals("R") || tags[i].equals("!") || tags[i].equals("L") ||
		tags[i].equals("E") || tags[i].equals("#") || tags[i].equals("^") || tags[i].equals("@") )
	{		
		if(!tags[i].equals("E"))
			tokens[i] = tokens[i].toLowerCase();
		sb.append(normalize(tokens[i]));
		sb.append(" ");
		sb.append(tags[i]);
		sb.append(" ");
	}	
}


/*StringBuilder sb = new StringBuilder();
sb.append(StringUtils.join(tokens));
sb.append("\t");

sb.append("TAGS");

if (!suppressTags) {
sb.append(StringUtils.join(tags));
sb.append("\t");
}

if (showConfidence) {
sb.append(StringUtils.join(confs));
sb.append("\t");
}

sb.append(inputLine);*/

//outputStream.println(sb.toString());
return sb.toString();
}

//delete symbols which occur more than twice together 
	private static String normalize(String str) {
		String norm_str = (str.length() > 2) ? str.substring(0, 2) : str;				
		for(int i = 2; i < str.length(); ++i) {
			if(str.charAt(i) != str.charAt(i-1) || str.charAt(i) != str.charAt(i-2)) {
				norm_str = norm_str.concat(((Character)str.charAt(i)).toString());
			}
		}
		return norm_str;
	}


///////////////////////////////////////////////////////////////////


public static void main(String[] args) throws IOException, ClassNotFoundException {


RunTagger tagger = new RunTagger();
tagger.runTagger("look at tagger");	
}



}