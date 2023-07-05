from flask import Flask, render_template, url_for, flash, redirect, request
from forms import RegistrationForm
import pymongo
import datetime
import os
import pickle
from bson.code import Code
from functools import reduce
import statistics
import time

application = Flask(__name__)
application.config['SECRET_KEY'] = 'F5TH7654KI890PL75RFV12TG78ILF421'

# cluster = pymongo.MongoClient(
#     "mongodb+srv://somesh:icdeskillstack@cluster0.odqp5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
# data = cluster["MapReduce_input"]
# database_input = data["MapReduce_input"]

cluster2 = pymongo.MongoClient("mongodb://suhas:Harrypotter123@cluster0-shard-00-00.d0hvs.mongodb.net:27017,"
                               "cluster0-shard-00-01.d0hvs.mongodb.net:27017,"
                               "cluster0-shard-00-02.d0hvs.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet"
                               "=atlas-ovwfa5-shard-0&authSource=admin&retryWrites=true&w=majority")
data2 = cluster2["MapReduce_input"]
database_output = data2["MapReduce_output"]
database_input = data2["MapReduce_input"]


class SerializeData:
    def __init__(self):
        self.nd_test = database_input.find_one({"id": "ndbench_testing"})
        self.nd_train = database_input.find_one({"id": "ndbench_training"})
        self.dvd_test = database_input.find_one({"id": "dvd_testing"})
        self.dvd_train = database_input.find_one({"id": "dvd_training"})


object = SerializeData()
with open('serialized_data.pickle', 'wb') as data:
    pickle.dump(object, data)

with open('serialized_data.pickle', 'rb') as data:
    serialized_object = pickle.load(data)

os.remove("serialized_data.pickle")


def mapreduce():
    map_function = Code("function(){emit(this.sample,{times:1});}")
    reduce_function = Code("function(sample, newarray){var value = {times:0};for(k=0; k<newarray.length; "
                           "k++){value.times += newarray[k].times;}return value;}")
    database_output.map_reduce(map_function, reduce_function, "MapReduce_output")


def get_data(benchtype, data_type, workload_metric, batchunit, rfw_id, batch_id, batch_size):
    current_data = {}
    dataremained = False

    if benchtype == "NDBench":
        if data_type == "Testing":
            current_data = serialized_object.nd_test
        if data_type == "Training":
            current_data = serialized_object.nd_train

    if benchtype == "DVD Store":
        if data_type == "Testing":
            current_data = serialized_object.dvd_test
        if data_type == "Training":
            current_data = serialized_object.dvd_train

    current_metric_data = current_data[workload_metric]

    Numberofbatches = (len(current_metric_data) // batchunit)
    if int(Numberofbatches) < Numberofbatches:
        Numberofbatches = int(Numberofbatches) + 1
        dataremained = True

    sliced_data = []
    start_index = 0
    end_index = batchunit

    for i in range(1, Numberofbatches):
        current_slice = current_metric_data[start_index:end_index]
        sliced_data.append(current_slice)
        start_index = end_index
        end_index = end_index + batchunit

    if dataremained:
        remaining_samples = len(current_metric_data) - (int(Numberofbatches - 1) * batchunit)
        remaining_samples_list = current_metric_data[
                                 (len(current_metric_data) - remaining_samples): len(current_metric_data)]
        sliced_data.append(remaining_samples_list)

    if (batch_id + batch_size - 1) > len(sliced_data):
        message = "Only " + str(
            Numberofbatches) + " batches are possible with the given input ! Try changing batch id or batch size."
        data_status = False
        return [sliced_data, message, data_status]

    else:
        requested_data = sliced_data[(batch_id - 1):(batch_id + batch_size) - 1]
        data_status = True
        message = "Successfully queried your data, RFW Id: " + str(rfw_id) + " | Last batch: " + str(
            Numberofbatches) + "."
        return [requested_data, message, data_status]


def map_reduce():
    mapped_data = []
    for detailed_data in database_output.find({}):
        mapped_data.append(detailed_data)
    return mapped_data


def mapper_1(a, b):
    if b >= a:
        return True
    else:
        return False


def mapper_2(a, b):
    if b <= a:
        return True
    else:
        return False


def reducer_1(a, b):
    return a + b


@application.route("/")
def run():
    return redirect(url_for("home"))


@application.route("/home", methods=['GET', 'POST'])
def home():
    form = RegistrationForm()
    if form.validate_on_submit():
        now = datetime.datetime.now()
        rfwid = now.strftime("%H%M%S")
        info = get_data(rfw_id=rfwid, benchtype=form.benchmark_type.data,
                        batch_id=form.batch_id.data, workload_metric=form.workload_metric.data,
                        batch_size=form.batch_size.data, batchunit=form.batch_unit.data,
                        data_type=form.data_type.data)
        required_data = info
        if info[2]:
            detailed_data = []
            for i in range(0, form.batch_size.data):
                detailed_sample = {"sample": required_data[0][i], "sample_id": form.batch_id.data + i,
                                   "metric": form.workload_metric.data, "benchtype": form.benchmark_type.data}
                detailed_data.append(detailed_sample)
            if request.method == "POST":
                flash(info[1], 'success')
                data_details = "Benchmark: " + str(detailed_data[0]["benchtype"]) + "  |  Metric: " + str(detailed_data[0]["metric"])
                flash(data_details, 'success')
            database_output.delete_many({})
            for sample in detailed_data:
                database_output.insert_one(sample)
            map_reduced_data = map_reduce()

            for sample_map_reduced in map_reduced_data:
                if False not in list(map(mapper_1, sample_map_reduced["sample"], [max(sample_map_reduced["sample"])])):
                    sample_map_reduced["maximum"] = max(sample_map_reduced["sample"])
                if False not in list(map(mapper_2, sample_map_reduced["sample"], [min(sample_map_reduced["sample"])])):
                    sample_map_reduced["minimum"] = min(sample_map_reduced["sample"])
                sample_map_reduced["sum"] = reduce(reducer_1, sample_map_reduced["sample"])
                sample_map_reduced["mean"] = sample_map_reduced["sum"]/len(sample_map_reduced["sample"])
                sample_map_reduced["SD"] = statistics.stdev(sample_map_reduced["sample"])
                sample_map_reduced["median"] = statistics.median(sample_map_reduced["sample"])
            for sample in map_reduced_data:
                sample["sample_occurrences"] = []
                for data in sample["sample"]:
                    occurenceString = str(data) + " - " + str(sample["sample"].count(data))
                    sample["sample_occurrences"].append(occurenceString)
            database_output.delete_many({})
            for sample in map_reduced_data:
                database_output.insert_one(sample)
            return render_template('home.html', title='Login', form=form, data=map_reduced_data)
        else:
            flash(info[1], 'danger')
            time.sleep(3)
            return redirect(url_for("home"))

    return render_template('index.html', title='Login', form=form)


if __name__ == '__main__':
    application.run(debug=True)
