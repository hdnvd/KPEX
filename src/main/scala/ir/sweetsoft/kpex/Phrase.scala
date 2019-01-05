package ir.sweetsoft.kpex

import ir.sweetsoft.nlp.NLPTools

class Phrase(ApplicationContext:KpexContext) extends KpexClass(ApplicationContext:KpexContext){

  private[this] var _words: Seq[String] = Seq()

  def words: Seq[String] = _words

  def words_=(value: Seq[String]): Unit = {
    _words = value
  }

  def Phrase: String = {
    val nlp=new NLPTools(appContext)
    nlp.getPhraseFromWords(words)
  }

  def Phrase_=(value: String): Unit = {
    val nlp=new NLPTools(appContext)
    words=nlp.GetStringWords(value)
  }
}
