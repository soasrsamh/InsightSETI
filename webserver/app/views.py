from flask import request
from flask import render_template
# jsonify creates a json representation of the response
from flask import jsonify

from app import app

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

# Setting up connections to cassandra

# Change the bolded text to your seed node public dns (no < or > symbols but keep quotations. Be careful to copy quotations as it might copy it as a special character and throw an error. Just delete the quotations and type them in and it should be fine. Also delete this comment line
cluster = Cluster([' insert my cassandra cluster dns here '])

# Change the bolded text to the keyspace which has the table you want to query. Same as above for < or > and quotations. Also delete this comment line
session = cluster.connect('hitplayground')

@app.route('/')
@app.route('/index')
def index():
#  user = { 'nickname': 'Miguel' } # fake user
#  mylist = [1,2,3,4]
#  return render_template("index.html", title = 'Home', user = user, mylist = mylist) 
  return "Hello, World! Go to /queries or /realtime."


@app.route('/api/<obsgroup>/<obsord>')
def get_hits(obsgroup, obsord):
       stmt = "SELECT * FROM hitInfo WHERE observationgroup="+str(obsgroup)+" and observationorder="+str(obsord)
#       stmt = "SELECT * FROM hitInfo"       
       #obsgroup = int(obsgroup)
       #obsord = int(obsord)
       #response = session.execute(stmt, parameters=[obsgroup, obsord])
       response = session.execute(stmt)
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{"observation group": x.observationgroup, "observation number": x.observationorder, "frequency": x.frequency, "SNR": x.snr, "driftrate": x.driftrate, "uncorrected frequency":x.uncorrectedfrequency} for x in response_list]
       return jsonify(hits=jsonresponse)

@app.route('/queries')
def queries():
 return render_template("queries.html")

@app.route("/queries", methods=['POST'])
def queries_post():
 querynum = request.form["querynum"]
 if querynum == "1":
  observationgroup1 = request.form["observationgroup1"]
  #email entered is in emailid and date selected in dropdown is in date variable respectively
  stmt = "SELECT * FROM groupInfo WHERE observationgroup="+str(observationgroup1)
  #stmt = "SELECT * FROM email WHERE id=%s and date=%s"
  #response = session.execute(stmt, parameters=[emailid, date])
  response = session.execute(stmt)
  response_list = []
  for val in response:
     response_list.append(val)
  #jsonresponse = [{"fname": x.fname, "lname": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
  jsonresponse = [{"observationgroup": x.observationgroup, "grouphit": x.grouphit, "source": x.source, "ra": x.ra, "dec": x.dec, "mjd": x.mjd, "filename1": x.filename1, "filename2": x.filename2, "filename3": x.filename3, "filename4": x.filename4, "filename5": x.filename5, "filename6": x.filename6} for x in response_list]
  return render_template("queriesop2.html", output=jsonresponse)
 else:
  observationgroup2 = request.form["observationgroup2"]
  observationorder2 = request.form["observationorder2"]
  #email entered is in emailid and date selected in dropdown is in date variable respectively
  stmt = "SELECT * FROM hitInfo WHERE observationgroup="+str(observationgroup2)+" and observationorder="+str(observationorder2)
  #stmt = "SELECT * FROM email WHERE id=%s and date=%s"
  #response = session.execute(stmt, parameters=[emailid, date])
  response = session.execute(stmt) 
  response_list = []
  for val in response:
     response_list.append(val)
  #jsonresponse = [{"fname": x.fname, "lname": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
  jsonresponse = [{"observationgroup": x.observationgroup, "observationorder": x.observationorder, "frequency": x.frequency, "SNR": x.snr, "driftrate": x.driftrate, "uncorrectedfrequency":x.uncorrectedfrequency} for x in response_list]
  return render_template("queriesop.html", output=jsonresponse)


@app.route('/realtime')
def realtime():
  stmt = "SELECT * FROM groupInfo WHERE grouphit>0 ALLOW FILTERING"
  response = session.execute(stmt)
  response_list = []
  for val in response:
     response_list.append(val)
  #jsonresponse = [{"fname": x.fname, "lname": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
  jsonresponse = [{"observationgroup": x.observationgroup, "grouphit": x.grouphit, "source": x.source, "ra": x.ra, "dec": x.dec, "mjd": x.mjd, "filename1": x.filename1, "filename2": x.filename2, "filename3": x.filename3, "filename4": x.filename4, "filename5": x.filename5, "filename6": x.filename6} for x in response_list]

  return render_template("realtimequeriesop2.html", output=jsonresponse)
  #return render_template("realtimequeriesop2.html")
