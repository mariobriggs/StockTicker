
/**
 * @author mbriggs
 */

import java.util._
import org.apache.kafka.clients.producer._
import scala.io.Source

object StockTickProducer extends App{
  
  
 val props = new java.util.Properties();
 props.put("bootstrap.servers", "localhost:9092");

 props.put("retries", "0");
 props.put("batch.size", "16384");
 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

 var startMsgId = 310
 val topic = args(0)
 val producer = new KafkaProducer[String,String](props)
 var i = 0
 
 var state =  "normal"
 
 val routine = new Thread(new Runnable {
    def run() {
     
      while (true) {
         val msg = "SYMB: IBM, price: 128, ts: " + System.currentTimeMillis()
        val ret = doRoutine(msg)
        if (ret == 1) {
          // wait for mpattern to finish
          do {
            println("waiting for mpat to finish")
            Thread sleep 1000
            
          } while (state != "normal")
          println("mpat finish")
        }
        else if (ret == 2) {
          return
        }
      }
    }
  })
 
 val mpattern = new Thread(new Runnable {
    def run() {
      // open a http port and listen for particular message
      for (ln <- Source.stdin.getLines) { 
        if (ln.equalsIgnoreCase("p")) {
          state = "mpat"
          generateMpattern()
          state = "normal"
        }
        else if (ln.equalsIgnoreCase("q")) {
          state = "exit"
          return
        }
      }
    }
  })
 
  mpattern.start()
  routine.start()
  routine.join()
  
  producer.close();
  
  
  
  def doRoutine(msg: String): Int = {
    for (i <- 0 to 2) {
      val rec = new ProducerRecord(topic, i, "mykey", msg)
      producer.send(rec)
      //println (msg)
      if (state.equalsIgnoreCase("mpat") ) {
        return 1
      } else if (state.equalsIgnoreCase("exit") ) {
        return 2
      }
        
      Thread sleep 900
      
      if (state.equalsIgnoreCase("mpat") )
        return 1
    }
    println ("generated tick")
    return 0
  }
  
  def generateMpattern() = {
     val mpatproducer = new KafkaProducer[String,String](props)
     
    Thread sleep 50
    val rise1 = "SYMB: IBM, price: 130, ts: " + System.currentTimeMillis()
    val drop1 = "SYMB: IBM, price: 129, ts: " + System.currentTimeMillis()
    val rise2 = "SYMB: IBM, price: 131, ts: " + System.currentTimeMillis()
    val rise3 = "SYMB: IBM, price: 132, ts: " + System.currentTimeMillis()
    val deep =  "SYMB: IBM, price: 125, ts: " + System.currentTimeMillis()
    
    mpatproducer.send(new ProducerRecord(topic, 1, "mykey", rise1))
    mpatproducer.send(new ProducerRecord(topic, 1, "mykey", drop1))
    mpatproducer.send(new ProducerRecord(topic, 1, "mykey", rise2))
    mpatproducer.send(new ProducerRecord(topic, 1, "mykey", rise3))
    mpatproducer.send(new ProducerRecord(topic, 1, "mykey", deep))
    
    mpatproducer.close()
    println("generated mpattern")
     
    //Thread sleep 500
  }
}
  
