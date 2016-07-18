
object Cells {
  import org.apache.spark._
  import org.apache.spark.SparkContext._
  import org.apache.spark.rdd._

  /* ... new cell ... */

  println("Change the file paths to the local whereever needed")
  val ratings_data = sparkContext.textFile("C:/Users/sanket.patil/Downloads/ml-1m/ratings.dat")
                          .map(line => line.split("::"))
                          .map(rating => (rating(0).toInt,rating(1).toString, rating(2).toInt,rating(3).toLong))
                        
                                 

  /* ... new cell ... */

  val movies_data_raw =sparkContext.textFile("C:/Users/sanket.patil/Downloads/ml-1m/movies.dat")
  val movies_data= movies_data_raw.map(line => line.split("::"))
                              .map(movie => (movie(0).toInt,movie(1)))

  /* ... new cell ... */

  val mapped_ratings = ratings_data.map{
                        case (userid,movieid,rating,time) => (movieid, 1)
                        }
                        .reduceByKey(_+_)
                        .collect()
                        .sortBy(-_._2)
                        .take(10)
                    
                   

  /* ... new cell ... */

  val totalcount=sc.parallelize(mapped_ratings)
  val x=movies_data.join(totalcount).map{
                                         case(x,(y,z))=>y
                                        }
  
                                    .collect()
  println("Top ten most viewed movies are :")
  x.foreach(println)

  /* ... new cell ... */

  val toprated_all=ratings_data.map{
                                 case (userid,movieid,rating,time) => ((movieid,rating),1)
                               }
                           .reduceByKey(_+_) 
                           .filter{
                               case ((movieid,rating),count) => count > 40 && rating==5
                           }
                           .collect()
                           .sortBy(-_._2)
                           
  
                           

  /* ... new cell ... */

  val toprated=toprated_all.take(20)

  /* ... new cell ... */

  val highest_rated=toprated.map{
     case ((movieid,rating),count) => (movieid,count)
  }
  val high_rated = sc.parallelize(highest_rated)
  val x=movies_data.join(high_rated).map{
                                         case(x,(y,z))=>y
                                        }
  
                                    .collect()
  println("Top Twenty highest rated movies are :")
  x.foreach(println)

  /* ... new cell ... */

  val movies_arr=toprated.map{
     case ((movieid,rating),count) => (movieid.toString)
  }
  val age_rated = sc.parallelize(highest_rated_for_age)

  /* ... new cell ... */

  val forage = ratings_data.filter(line => movies_arr.exists(line._2.contains) && line._3==5)
  val topmoviesusers=forage.map{
                                case(userid,movieid,rating,time) => (userid,movieid)
                              }
                           

  /* ... new cell ... */

  val users_data = sparkContext.textFile("C:/Users/sanket.patil/Downloads/ml-1m/users.dat")
                          .map(line => line.split("::"))
                          .map(user => (user(0).toInt,user(2).toString))

  /* ... new cell ... */

  val x=topmoviesusers.join(users_data)
  println("Number of views by young age group of Top rated films ")
  val young= x.filter{
                  case (userid,(movieid,age)) => age.toInt <20 
  }
        .count
  println("Number of views by young adult age group of Top rated films ")
  val yadult= x.filter{
                  case (userid,(movieid,age)) => age.toInt >20 && age.toInt <40 
  }
        .count
  println("Number of views by adult age group of Top rated films ")
  val adult= x.filter{
                  case (userid,(movieid,age)) => age.toInt <40
  }
        .count

  /* ... new cell ... */

  println("Top 10 bad critics with 40 reviews")
  
  val bad_critics=ratings_data.map{
                                 case (userid,movieid,rating,time) => ((userid,rating),1)
                               }
                           .reduceByKey(_+_) 
                            .filter{
                               case ((userid,rating),count) => count > 40 && rating==1
                           }
                           .sortBy(-_._2)
                           .map{
                             case ((userid,rating),count) => (userid) 
                            }
                           .take(10)
  bad_critics.foreach(println)
                           

  /* ... new cell ... */
}
                  