# SETI Breakthrough Listen
Streaming Detection of Extraterrestrial Anomalous Signals
Insight Data Engineering Fellow September 2017


Over the last couple weeks, I built a streaming anomalous signal detection pipeline for high level SETI signal data. 
SETI is the organization that searches for extraterrestrial signals. 

Figure 1:

Stream processing is motivated by the value that repeated observations of anomalous signals can bring. 
Real-time notifications would give researchers the option to immediately re-measure stars that have been identified as anomalous. 
SETI is looking for a needle in a haystack, and if they observe a very interesting needle-like signal, it is of great value to immediately re-measuring that anomaly within the same night.

To this end, I built a streaming pipeline to ingest with Kafka high-level signal data from observatory producers, to identify anomalous signals with Spark Streaming, and to save results in a Cassandra database that can be queried through my Flask web interface, which also has realtime notification of anomalous signals. 
Customized Spark batch processing scripts can also be applied to the data stored in Cassandra.

Figure 2:

The data look something like this: Each observation of a star has many signals and each signal has many properties.
To be considered anomalous, a signal must first exceed simple threshold filters on Property2 and Property3.
Then, to filter out earth-originating signals, signals must be a member of all observations when the telescope is pointed directly ON the a star or galaxy, but not a member of any of the observations when the telescope is pointed OFF of the star. 
Because this second criteria involves matching signals between different observations, a computationally-intensive join operation is necessary.

Figure 3:

To imagine this challenge, consider matching properties that are +/- 1 units of one another between to two lists.
Using the default join in scala, each element of the first list will be compared with each element of the second list, cartesian pairwise style, which is a slow N2 operation when there are millions of rows in each list.
I implemented a more efficient range-join using a bucketing trick, where elements from each list are bucketed and elements are only joined with items within each bucket.
I measured this to be 300 times faster for range-joining two sets with a million items each.
To put that in perspective, a join that would take over two days to run, takes 10 minutes. 

Figure 4:
Matching signals is even more difficult when you want to match based on multiple property values.
For example, when you join based on if two sets of properties are within 1 unit distance of each other, the bucketing trick in the previous slide does not directly apply. 
So, I implemented a probabilistic hashing trick to make the join tractable.
However, this approach has a trade-off between speed and accuracy. 

Figure 5:

I created two web pages. 
The first allows one to input values to query my database.
The second updates every 10 seconds displaying newly identified anomalous signals in real-time within minutes, notifying the researcher if any stars are worth immediately remeasuring within the same night.

Figure 6:

And finally, about me, I’m Sarah Howell.
My background is in physics research. 
I have a plethora of interests from art to slacklining to biking.
For example, here’s a picture of me when I biked solo 3000 miles coast to coast across the US earlier this year.
Thank you for your time!

