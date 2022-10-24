# coding=utf-8
""" Load class of the ETL. """
import json
from etl_src.configs.core import etl_config, RESULTS_DIR
from etl_src.services import local_data_collector

class LoadJob:
    """_summary_
    """
    def __init__(self) -> None:
        self.df_all_data = local_data_collector.get_all_data_as_dataframe()
        self.all_data_as_dict = dict()
        self.journal_max_diff_drugs_dict = dict()
    
    def get_results_as_dict(self) -> None:
        """_summary_
        """
        for journal, journal_gp in self.df_all_data.groupby("journal"):
            self.all_data_as_dict[journal] = dict()
            for type, type_gp in journal_gp.groupby("type"):
                self.all_data_as_dict[journal][type] = dict()
                for title, title_gp in type_gp.groupby("title"):
                    self.all_data_as_dict[journal][type][title] = dict()
                    for drug, drug_gp in title_gp.groupby("drug"):
                        self.all_data_as_dict[
                            journal
                            ][
                                type
                                ][
                                    title
                                    ][drug] = list(drug_gp["date"].unique())
              
    def serialize_final_results_as_json(self) -> None:
        """
        """
        # serialize final results 
        with open(
            RESULTS_DIR / etl_config.files_input_data_config.final_results,
            "w",
        ) as write_json_file:
            json.dump(
                self.all_data_as_dict,
                write_json_file,
                indent=4
            )  
 
    def extract_journal_with_most_distinct_drugs(self) -> None:
        """
        """
        dict_journal_drugs = { jrn:[] for jrn in self.all_data_as_dict}
        for journal, j_contains in self.all_data_as_dict.items():
            for _, t_contains in j_contains.items():
                for _, tit_contains in t_contains.items():
                    for drug in tit_contains:
                        dict_journal_drugs[journal].append(drug)
        
        # for jr, drugs in dict_journal_drugs.items():
        #     print(jr, drugs)
            
        dict_journal_nbr_distinct = {
            journal : len(set(dict_journal_drugs[journal]))
            for journal in dict_journal_drugs
        }   
        max_nb_drugs = max(dict_journal_nbr_distinct.values())    
        
        self.journal_max_diff_drugs_dict = {
            journal: nb_drugs_diff
            for journal, nb_drugs_diff in dict_journal_nbr_distinct.items()
            if nb_drugs_diff == max_nb_drugs
        }
  
    def serialize_journal_max_diff_drugs(self) -> None:
        """_summary_
        """
        # serialize results of journal with max diff drugs
        with open(
            RESULTS_DIR / etl_config.files_input_data_config.resutls_jounral_max_diff_drugs,
            "w"
        ) as write_json_file:
            json.dump(
                self.journal_max_diff_drugs_dict,
                write_json_file,
                indent=4
            )
    def run_worker(self) -> None:
        """_summary_
        """
        self.get_results_as_dict()
        self.serialize_final_results_as_json()
        self.extract_journal_with_most_distinct_drugs()
        self.serialize_journal_max_diff_drugs()