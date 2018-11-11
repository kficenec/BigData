package wm.kdficenec

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import java.io._

//recommends 10 friends for each user, ordered by most mutual connections to least.
//or, if they have less than 10 second degree connections, just outputs that many 
// (still in order of most mutual connections to least).
//or, outputs "no friends" if they don't have any friends (and therefore have no 
//  second degree connections).
//if 2 users have the same number of mutual connections, then the recommendation tie
//is broken in order of ascending user ID number.

object friendRec {

  //parseline to yield friend pairs
  def parseLine(line: String) = { 
      //The userID is separated from a user's list of friends by a tab "\t"
      val fields = line.split("\t")
      //convert the userID to an integer value.
      val userID = fields(0).toInt
      //if else to handle users who have no friends
      //if they have a list of friends fields(1) will be defined.
      if (fields.isDefinedAt(1)) {
        //the friend list is separated by commas.
        val friends = fields(1).split(",").toList  
        //scala code for a double-nested for loop, eliminating the combination where
        //you would be pairing a friendID with themselves.
        //tag a 1 on the end to count that those friends showed up together once. 
        //(will be used later in code to count number of mutual connections... that is,
        //   friends showing up together in the same friend list)
        for (fr <- friends; fr1 <- friends; if (fr.toInt != fr1.toInt)) yield((fr.toInt, fr1.toInt),1)
      }
      //handling when a user has no friends. could not use zero b/c
      //that is someone else's userID.
      else {
        List(((userID, -1), -1))
      }
  }
  
  //parse line to yield already friends (with zero value)
  def aFri(line: String) = {
      val fields = line.split("\t")
      val userID = fields(0).toInt
      if (fields.isDefinedAt(1)) {
        val friends = fields(1).split(",").toList 
        //if a user is already friends with someone, we don't want
        //to recommend that they friend that person, so we want their IDs
        //paired together as a key, with a value of zero.
        for (fr <- friends) yield((userID, fr.toInt),0)
      }
      //again handling when a user has no friends
      else {
        List(((userID, -1), -1))
      }
  }
    
  
   /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    //Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine, named "friendRec"
    val sc = new SparkContext("local[*]", "friendRec")

    // Load up each line of the ratings data into an RDD
    val data = sc.textFile("../input-f.txt")
    
    //make an Rdd for friend cnxs (people who show up together in other user's
    //friend lists) 
    val rdd = data.flatMap(parseLine)
    //...and an Rdd for users who are already friends.
    val alreadyFriends = data.flatMap(aFri)
    //all entries are in the form ((ID, ID), num) where num = 1 for friend pairs, 
    //0 for already friends and -1 for no friends.
    
    //combine the 2 rdds into 1.
    val fullRdd = rdd.union(alreadyFriends)
 
    //reduce by key.  x is the running total and y is the current value.
    //the else will either be (1 + 1*runningTotal) or, if a user is already
    //friends with someone, then an instance of (0 + 0*runningTotal) will come in, setting the running total
    //to zero, then any other times that pair (who is already friends) comes up, the else if clause will
    //catch it and keep it at zero.
    val friendCount = fullRdd.reduceByKey((x,y) => if (y == -1) (y) else if (x==0) (x) else (y + x*y) )
    
    //will filter out user-alreadyFriends & user-user (which was set to zero above if a user's userID was Also in their friend's list)
    val friendCountFiltered = friendCount.filter(_._2 != 0)
    //val freindCountFiltered2 = friendCountFiltered.mapValues(x => if (x == -1) ("no friends") else (x))
    
    //reorganize, so that the userID alone is the key (not the pair)
    val friendCounts = friendCountFiltered.map(x => (x._1._1, (x._2, x._1._2)))
    
    //I first ordered by ascending ID number (of friend recs) and then by number cnxs;
    //this made it so that the same cnx ties were broken in order of ascendingID number.
    //the sortBy(Negative _._2._1) is to get the friend rec in Descending (most mutual cnxs first) order.
    //I then mapped to just the userID and their friend recs (so removed the num mutual cnxs from showing.  
    //and lastly grouped by key:
    //val friendRecsByUser = friendCounts.sortBy(_._2._2).sortBy(- _._2._1).map(x => (x._1, x._2._2)).groupByKey()
    //use the line below instead of above if you want "no friends" written instead of -1.
    val friendRecsByUser = friendCounts.sortBy(_._2._2).sortBy(- _._2._1).mapValues(x => if (x._1 == -1) ((x._2, "no friends")) else (x)).map(x => (x._1, x._2._2)).groupByKey()
    
    //take top 10, and sort by the userID keys.
    val friendRecsByUserTopTen = friendRecsByUser.map(x => (x._1, x._2.take(10))).sortByKey()
    
    //output with ID <tab> recommendations
    val output = friendRecsByUserTopTen.map(x => x._1 + "\t" + x._2.mkString(","))
    
    //collect results, I think this is when it will actually start executing.
    val results = output.collect()
    
    //print the results to the console.
    results.foreach(println)
    
    //also write the results to a text file.
    val file = "friendRec.txt"    
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- results) {
        writer.write(x + "\r\n")
                    }
    writer.close()  

  }
}

