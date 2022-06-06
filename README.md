# RedditAnalysis
Code basis for my bachelor thesis " The Effects of Group Size on the Activity in Online Communities: Evidence from Reddit"

# Disclaimer
The code was written to achive the goal of retrieving data for my bachelor thesis. It is not written to be a libary or be anything but a script to get the data for this specific project.


comments.py
- downloads the dumps from https://files.pushshift.io/reddit/comments/
  -  multithreaded download, unpacking and processing dumps
  -  resumeable download
  -  filters dump by subreddits

Outputs: all comments from specified subreddits in a given timefram


posts.py
- parses pushshift iteratively

Outputs: all posts from specified subreddits in a given timefram


/analysis
- contains all the R analysis code and results
