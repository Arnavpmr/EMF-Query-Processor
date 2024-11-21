import json, re
from helpers import parse_list_input, tts

class MFQueryProcessor:
    def __init__(self, inputs={}):
        self.inputs = inputs
    
    # Get the input from a json file and load data into inputs
    def get_input_from_file(self, filename):
        with open(f'inputs/{filename}.json', 'r') as f:
            self.inputs = json.load(f)
        
        self.__preprocess_for_avrg()
    
    # Get the input from stdin and load data into inputs
    def get_input_from_stdin(self):
        self.inputs["selections"] = parse_list_input(input("Enter the selections: "))
        self.inputs["n"] = int(input("Enter the value of n: "))
        self.inputs["grouping_attrs"] = parse_list_input(input("Enter the grouping attributes: "))
        self.inputs["aggregates"] = parse_list_input(input("Enter the aggregates: "))
        self.inputs["pred_list"] = parse_list_input(input("Enter the predicate list: "))
        self.inputs["having_pred"] = input("Enter the having predicate: ")

        self.__preprocess_for_avrg()
    
    # Returns the string for initializing each aggregate field in mf class
    def __get_mf_class_assignment_from_aggr(self, full_aggr):
        aggr = full_aggr.split("_")[0]
        if aggr == "max":
            return f"{full_aggr}=float('-inf')"
        elif aggr == "min":
            return f"{full_aggr}=float('inf')"
        else:
            return f"{full_aggr}=0"

    # Returns the string for initializing the mf class
    def initialize_mf_class(self):
        if not self.inputs["aggregates"]:
            return (
                "class H:\n"
                "\tdummy_field=0\n"
            )

        mf_class = (
             "class H:\n"
            f"\t{"\n\t"
               .join(
                   list(map(self.__get_mf_class_assignment_from_aggr, self.inputs["aggregates"]))
                )
            }"
            "\n"
        )

        return mf_class

    def getIthAggregates(self, i):
        return list(filter(lambda x: int(x.split("_")[1]) == i, self.inputs["aggregates"]))
    
    def getIthPredicate(self, i):
        predicates = list(filter(lambda x: int(x.split(".")[0]) == i, self.inputs["pred_list"]))

        return predicates[0] if predicates else ""
    
    def __preprocess_for_avrg(self):
        non_avrg_aggrs = set(list(filter(lambda x: x.split("_")[0] != "avrg", self.inputs["aggregates"])))
        avrg_aggrs = list(filter(lambda x: x.split("_")[0] == "avrg", self.inputs["aggregates"]))

        for avrg_aggr in avrg_aggrs:
            aggr, i, attr = avrg_aggr.split("_")
            if f"sum_{i}_{attr}" not in non_avrg_aggrs:
                non_avrg_aggrs.add(f"sum_{i}_{attr}")
            if f"count_{i}_{attr}" not in non_avrg_aggrs:
                non_avrg_aggrs.add(f"count_{i}_{attr}")
        
        self.inputs["aggregates"] = list(non_avrg_aggrs) + avrg_aggrs
    
    def __pred_to_py_exp(self, pred):
        attr_pattern = r"(\d+)\.(\w+)"
        attr_replacement = r"row['\2']"

        return re.sub(attr_pattern, attr_replacement, pred)
    
    def __having_pred_to_py_exp(self, pred):
        aggr_pattern = r"(\w+_\d+_\w+)"
        aggr_replacement = r"val.\1"

        return re.sub(aggr_pattern, aggr_replacement, pred)

    def __get_assignment_from_aggr(self, full_aggr):
        aggr, i, attr = full_aggr.split("_")

        if aggr == "count":
            return " += 1"
        elif aggr == "sum":
            return f" += row['{attr}']"
        elif aggr == "avrg":
            return f" = mf_struct[grouping_attrs_key].sum_{i}_{attr}/mf_struct[grouping_attrs_key].count_{i}_{attr}"
        elif aggr in ["max", "min"]:
            return f" = {aggr}(mf_struct[grouping_attrs_key].{full_aggr}, row['{attr}'])"

    def generate_aggr_assignments(self, aggrs, tab_count):
        output = []

        for aggr in aggrs:
            output.append(f"mf_struct[grouping_attrs_key].{aggr}{self.__get_assignment_from_aggr(aggr)}")

        return f"{tts(tab_count)}{f"\n{tts(tab_count)}".join(output)}\n"
    
    # Returns the string for the dictionary containing the output columns to be added as a row in the output table
    def generate_output_loop(self):
        grouping_attr_selections = list(filter(lambda x: x in self.inputs["grouping_attrs"], self.inputs["selections"]))
        aggr_selections = list(filter(lambda x: x in self.inputs["aggregates"], self.inputs["selections"]))

        loop = "for key, val in mf_struct.items():\n"
        having_pred = self.inputs["having_pred"]
        tabs = 2

        keys_outputs = ""
        aggr_outputs = f"{', '.join(map(lambda x: f"'{x}':val.{x}", aggr_selections))}"

        if len(self.inputs["grouping_attrs"]) == 1:
            keys_outputs = f"'{grouping_attr_selections[0]}':key"
        else:
            keys_outputs_list = []

            for i, attr_selection in enumerate(grouping_attr_selections):
                keys_outputs_list.append(f"'{attr_selection}':key[{i}]")

            keys_outputs = f"{', '.join(keys_outputs_list)}"

        col_keys = f"{keys_outputs}, {aggr_outputs}"

        if having_pred:
            having_pred = self.__having_pred_to_py_exp(having_pred)
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
        loop = self.__generate_var_loop_base(1)
        aggrs = self.getIthAggregates(0)
        predicate = self.getIthPredicate(0)
        tabs = 2

        if predicate:
            predicate = self.__pred_to_py_exp(predicate)
            loop += f"{tts(tabs)}if {predicate}:\n"
            tabs += 1
        
        loop += (
            f"{tts(tabs)}if grouping_attrs_key not in mf_struct:\n"
            f"{tts(tabs+1)}mf_struct[grouping_attrs_key] = H()\n\n"
        )

        if aggrs:
            loop += self.generate_aggr_assignments(aggrs, tabs)

        return loop

    def generate_grouping_vars_loop(self):
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