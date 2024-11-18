import json
from helpers import parse_list_input

class MFQueryProcessor:
    def __init__(self, inputs={}):
        self.inputs = inputs
    
    def get_input_from_file(self, filename):
        with open(f'inputs/{filename}', 'r') as f:
            self.inputs = json.load(f)
    
    def get_input_from_stdin(self):
        self.inputs["selections"] = parse_list_input(input("Enter the selections: "))
        self.inputs["n"] = int(input("Enter the value of n: "))
        self.inputs["grouping_attrs"] = parse_list_input(input("Enter the grouping attributes: "))
        self.inputs["aggregates"] = parse_list_input(input("Enter the aggregates: "))
        self.inputs["pred_list"] = parse_list_input(input("Enter the predicate list: "))