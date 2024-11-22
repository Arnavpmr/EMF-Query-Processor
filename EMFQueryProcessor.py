import json, re
from helpers import parse_list_input, tts

class EMFQueryProcessor:
    def __init__(self, inputs={}):
        self.inputs = inputs
    
    # Get the input from a json file and load data into inputs
    def get_input_from_file(self, filename):
        with open(f'inputs/{filename}.json', 'r') as f:
            self.inputs = json.load(f)
        
        self.__preprocess_for_avg()
    
    # Get the input from stdin and load data into inputs
    def get_input_from_stdin(self):
        self.inputs["selections"] = parse_list_input(input("Enter the selections: "))
        self.inputs["n"] = int(input("Enter the value of n: "))
        self.inputs["grouping_attrs"] = parse_list_input(input("Enter the grouping attributes: "))
        self.inputs["aggregates"] = parse_list_input(input("Enter the aggregates: "))
        self.inputs["pred_list"] = parse_list_input(input("Enter the predicate list: "))
        self.inputs["having_pred"] = input("Enter the having predicate: ")

        self.__preprocess_for_avg()
    
    # Returns the string for initializing each aggregate field in mf class
    def __get_mf_class_assignment_from_aggr(self, full_aggr):
        aggr = full_aggr.split("_")[0]
        if aggr == "max":
            return f"{full_aggr}=float('-inf')"
        elif aggr == "min":
            return f"{full_aggr}=float('inf')"
        else:
            return f"{full_aggr}=0"
    
    # Returns the string for initializing each grouping attribute in mf class
    def __get_mf_class_assignment_from_attr(self, attr):
        if attr in ["day", "month", "year", "quant"]:
            return f"{attr}=0"
        else:
            return f"{attr}=''"

    # Returns the string for initializing the mf class
    def initialize_mf_class(self):
        grouping_attr_assignments = list(map(self.__get_mf_class_assignment_from_attr, self.inputs["grouping_attrs"]))
        aggr_assignments = list(map(self.__get_mf_class_assignment_from_aggr, self.inputs["aggregates"]))

        mf_class = (
             "class H:\n"
            f"\t{"\n\t"
               .join(
                   grouping_attr_assignments + aggr_assignments
                )
            }"
            "\n"
        )

        return mf_class

    def getIthAggregates(self, i):
        return list(filter(lambda x: int(x.split("_")[1]) == i, self.inputs["aggregates"]))
    
    def getIthPredicate(self, i):
        predicates = list(filter(lambda x: int(x[0]) == i, self.inputs["pred_list"]))
        return predicates[0] if predicates else ""
    
    def __preprocess_for_avg(self):
        non_avg_aggrs = set(list(filter(lambda x: x.split("_")[0] != "avg", self.inputs["aggregates"])))
        avg_aggrs = list(filter(lambda x: x.split("_")[0] == "avg", self.inputs["aggregates"]))

        for avg_aggr in avg_aggrs:
            _, i, attr = avg_aggr.split("_")
            if f"sum_{i}_{attr}" not in non_avg_aggrs:
                non_avg_aggrs.add(f"sum_{i}_{attr}")
            if f"count_{i}_{attr}" not in non_avg_aggrs:
                non_avg_aggrs.add(f"count_{i}_{attr}")
        
        self.inputs["aggregates"] = list(non_avg_aggrs) + avg_aggrs
    
    def __pred_to_py_exp(self, pred):
        main_attr_pattern = r"0\.(\w+)"
        var_attr_pattern = r"\d+\.(\w+)"
        aggr_pattern = r"(\w+_\d+_\w+)"
        main_attr_replacement = r"h.\1"
        var_attr_replacement = r"row['\1']"

        pred = re.sub(main_attr_pattern, main_attr_replacement, pred)
        pred = re.sub(var_attr_pattern, var_attr_replacement, pred)
        return re.sub(aggr_pattern, r"h.\1", pred)
    
    def __get_where_pred_py_exp(self):
        pred = self.getIthPredicate(0)
        return re.sub(r"0\.(\w+)", r"row['\1']", pred)

    def __get_having_pred_py_exp(self):
        return re.sub(r"(\w+_\d+_\w+)", r"h.\1", self.inputs["having_pred"])

    def __get_assignment_from_aggr(self, full_aggr):
        aggr, i, attr = full_aggr.split("_")

        if aggr == "count":
            return " += 1"
        elif aggr == "sum":
            return f" += row['{attr}']"
        elif aggr == "avg":
            return f" = h.sum_{i}_{attr}/h.count_{i}_{attr}"
        elif aggr in ["max", "min"]:
            return f" = {aggr}(h.{full_aggr}, row['{attr}'])"

    def generate_aggr_assignments(self, aggrs, tab_count):
        output = []

        for aggr in aggrs:
            output.append(f"h.{aggr}{self.__get_assignment_from_aggr(aggr)}")

        return f"{tts(tab_count)}{f"\n{tts(tab_count)}".join(output)}\n"
    
    def generate_grouping_attr_assignments(self, tabs):
        return f"{tts(tabs)}{f"\n{tts(tabs)}".join(
            map(
                lambda x: f"h.{x} = row['{x}']",
                self.inputs["grouping_attrs"]
            )
        )}\n"
    
    # Returns the string for the dictionary containing the output columns to be added as a row in the output table
    def generate_output_loop(self):
        loop = "for h in mf_struct:\n"
        having_pred = self.__get_having_pred_py_exp()
        tabs = 2
        col_keys = ", ".join(map(lambda x: f"'{x}': h.{x}", self.inputs["selections"]))

        if having_pred:
            loop += f"{tts(tabs)}if {having_pred}:\n"
            tabs += 1
        
        loop += f"{tts(tabs)}output_table.append({{{col_keys}}})\n"

        return loop
    
    def __generate_var_loop_base(self, tabs):
        return (
            "for row in cur:\n"
            f"{tts(tabs+1)}grouping_attrs_key = ({", ".join(list(map(lambda x: f"row['{x}']", self.inputs["grouping_attrs"])))})\n\n"
        )
    
    def generate_main_var_loop(self):
        loop = (
            "for row in cur:\n"
            f"{tts(2)}grouping_attrs_key = ({", ".join(list(map(lambda x: f"row['{x}']", self.inputs["grouping_attrs"])))})\n\n"
        )
        aggrs = self.getIthAggregates(0)
        predicate = self.__get_where_pred_py_exp()
        tabs = 2

        if predicate:
            loop += f"{tts(tabs)}if {predicate}:\n"
            tabs += 1
        
        loop += (
            f"{tts(tabs)}if grouping_attrs_key not in mf_struct_set:\n"
            f"{tts(tabs+1)}mf_struct_set.add(grouping_attrs_key)\n\n"
            f"{tts(tabs+1)}h = H()\n"
        )

        loop += self.generate_grouping_attr_assignments(tabs+1)
        if aggrs:
            loop += self.generate_aggr_assignments(aggrs, tabs+1)

        loop += f"{tts(tabs+1)}mf_struct.append(h)\n"

        return loop
    
    def generate_grouping_vars_loop(self, vars):
        loop = (
            "for row in cur:\n"
            f"{tts(2)}for h in mf_struct:\n"
        )

        for var in vars:
            tabs = 3
            aggrs = self.getIthAggregates(var)
            pred = self.getIthPredicate(var)

            if pred:
                pred = self.__pred_to_py_exp(pred)
                loop += f"{tts(tabs)}if {pred}:\n"
                tabs += 1

            if aggrs:
                loop += self.generate_aggr_assignments(aggrs, tabs)
        
        return loop

    def generate_grouping_vars_loop2(self):
        if self.inputs["n"] == 0:
            return ""

        loop = self.__generate_var_loop_base(1)
        main_var_pred = self.getIthPredicate(0)
        tabs = 2

        if main_var_pred:
            main_var_pred = self.__pred_to_py_exp(main_var_pred)
            loop += f"{tts(tabs)}if {main_var_pred}:\n"
            tabs += 1

        for i in range(1, self.inputs["n"] + 1):
            aggrs = self.getIthAggregates(i)
            pred = self.getIthPredicate(i)
            loop_tabs = tabs

            if pred and aggrs:
                pred = self.__pred_to_py_exp(pred)
                loop += f"{tts(loop_tabs)}if {pred}:\n"
                loop_tabs += 1

            if aggrs:
                loop += self.generate_aggr_assignments(aggrs, loop_tabs)

        return loop