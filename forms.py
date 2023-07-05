from flask_wtf import FlaskForm
from wtforms import SubmitField, SelectField, IntegerField
from wtforms.validators import DataRequired, NumberRange


class RegistrationForm(FlaskForm):
    benchmark_type = SelectField('Benchmark Type', choices=["DVD Store", "NDBench"], validators=[DataRequired()])
    workload_metric = SelectField('Workload Metric', choices=["CPUUtilization_Average", "NetworkIn_Average", "NetworkOut_Average", "MemoryUtilization_Average"],
                                  validators=[DataRequired()])
    batch_unit = IntegerField('Batch Unit', validators=[DataRequired(),
                                                        NumberRange(min=1, message="Enter a valid batch unit")])
    batch_id = IntegerField('Batch Id',
                            validators=[DataRequired(), NumberRange(min=1, message="Enter a valid number")])
    batch_size = IntegerField('Batch Size',
                              validators=[DataRequired(), NumberRange(min=1, message="Enter a valid batch size")])
    data_type = SelectField('Data Type', choices=["Testing", "Training"], validators=[DataRequired()])
    submit = SubmitField('Submit')
