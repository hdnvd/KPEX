package ir.sweetsoft.nlp

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations._
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.process.Stemmer
import edu.stanford.nlp.util.CoreMap
import ir.sweetsoft.common.SweetOut
import ir.sweetsoft.kpex.{KpexClass, KpexContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class NLPTools(ApplicationContext:KpexContext) extends KpexClass(ApplicationContext:KpexContext) {
  val stopWords = Set("a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "computer", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "isnt", "doesnt", "arent", "wasnt", "couldnt", "cant")
  def replaceExtraCharactersFromWord(InputString:String): String =
  {
    var Result=InputString

    Result = Result.replace("ö","o")
    Result = Result.replace("æ","")
    Result = Result.replace("§","")
    Result = Result.replace("°","")
    Result = Result.replace(".","")
    Result = Result.replace(",","")
    Result = Result.replace("@","")
    Result = Result.replace("{","")
    Result = Result.replace("}","")
    Result = Result.replace("[","")
    Result = Result.replace("]","")
    Result = Result.replace("(","")
    Result = Result.replace(")","")
    Result = Result.replace("?","")
    Result = Result.replace(":","")
    Result = Result.replace("!","")
    Result = Result.replace("'","")
    Result = Result.replace("`","")
    Result = Result.replace("\"","")
    Result
  }
//  private def NormalizeWordAndLemmatize(Word: String): String = {
//    val stemmer = new Stemmer()
//    var ResultString = replaceExtraCharactersFromWord(Word)
//    ResultString = stemmer.stem(ResultString)
//    val wordLemmas = plainTextToLemmas(ResultString)
//    if (wordLemmas != null && !wordLemmas.isEmpty)
//      ResultString = wordLemmas(0)
//    else
//      ResultString = ""
//    ResultString
//  }

  private def NormalizeWord(Word: String): String = {
//    val stemmer = new Stemmer()
    var ResultString = replaceExtraCharactersFromWord(Word)
//    ResultString = stemmer.stem(ResultString)
//    val wordLemmas = plainTextToLemmas(ResultString)
//    if (wordLemmas != null && !wordLemmas.isEmpty)
//      ResultString = wordLemmas(0)
//    else
//      ResultString = ""
    ResultString
  }
  def GetNormalizedAndLemmatizedWord(Word: String): String = {
    val stemmer = new Stemmer()
    var ResultString = replaceExtraCharactersFromWord(Word)
    if(isStopWord(Word))
      ResultString = ""
    else
      ResultString=getWordLemmatizedForm(ResultString)
    ResultString=stemmer.stem(ResultString)
    ResultString
  }
  def stem(WordName:String): String =
  {
    val stemmer=new Stemmer()
    stemmer.stem(WordName)
  }
  def isStopWord(word: String): Boolean = {
    val stopWordsList = stopWords.toList
    (stopWordsList.contains(word) && word.toString.matches("^[-a-zA-Z]+"))

  }
  def GetStringWords(inputString:String): Seq[String] =
  {
    var inputStringWords = inputString.split("\\s+").filterNot(_ == "")
    inputStringWords
  }
  def addToLemmatizationMap(Sentences: String): Unit = {

    var theText=Sentences
    if (Sentences.length > 1 && !Sentences.substring(Sentences.length - 1, Sentences.length).equals(".")){
      theText=theText+"."
    }
    val sentences = GetAnnotation(theText)
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      var lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 0 && replaceExtraCharactersFromWord(lemma).trim.length>0){
        appContext.LemmatizationMap=appContext.LemmatizationMap+(token.get(classOf[TextAnnotation]).toLowerCase.trim->lemma)
      }

    }
  }
  def plainTextToNormalLemmas(Sentences: String, LabelPartOfSpeech: Boolean): Array[String] = {// Used Once and with sentence

    SweetOut.printLine("Number of Words in Sentence:"+GetStringWords(Sentences).length,1)
    var theText=Sentences
    if (Sentences.length > 1 && !Sentences.substring(Sentences.length - 1, Sentences.length).equals(".")){
      theText=theText+"."
    }
    val sentences = GetAnnotation(theText)
    val lemmas = new ArrayBuffer[String]()
    var TokenCount=0
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      var lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 0 && replaceExtraCharactersFromWord(lemma).trim.length>0){
        TokenCount=TokenCount+1
        if (!stopWords.contains(lemma)) {
          val Pos = token.get(classOf[PartOfSpeechAnnotation])

          if (LabelPartOfSpeech)
            lemmas += Pos + "_" + NormalizeWord(lemma.toLowerCase)
          else
            lemmas += NormalizeWord(lemma.toLowerCase)
        }
      }

    }
    if(lemmas.nonEmpty)
      {
        SweetOut.printLine("Number of Words in Sentence After Lemmatization :"+TokenCount,1)
        lemmas.toArray
      }
    else
      Array()
  }
  def getWordLemmatizedForm(word:String): String =
  {
    val normalWord=word.toLowerCase.trim
    if(appContext.LemmatizationMap.keySet.exists(_ .equals(normalWord)))
      appContext.LemmatizationMap(normalWord)
    else
      word
  }
  def getTextPOSes(text: String): Array[String] = {
    val sentences = GetAnnotation(text)
    val Poses = new ArrayBuffer[String]()
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val Pos = token.get(classOf[PartOfSpeechAnnotation])
      Poses += Pos.toLowerCase
    }
    Poses.toArray
  }

  private def GetAnnotation(text: String): java.util.List[CoreMap] = {
    GetAnnotation(text,"tokenize, ssplit, pos, lemma")
  }
  private def GetAnnotation(text: String,annotators: String): java.util.List[CoreMap] = {
    val props = new Properties()
    props.put("annotators", annotators)
    val pipeline = new StanfordCoreNLP(props)
    val theText=text.trim
    val doc = new Annotation(theText)
    pipeline.annotate(doc)
    val sentences = doc.get(classOf[SentencesAnnotation])
    sentences
  }
}