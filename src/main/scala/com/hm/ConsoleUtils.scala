package com.hm

/**
  * Created by vishnu on 3/22/17.
  */
object ConsoleUtils {

  def consoleStatusWriter(text: String): Unit = System.out.write(("\r" + text).getBytes)
}
