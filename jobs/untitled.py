class Generator:
   def __init___(path_to_json: str):
     pass
   
   def set_outcome(outcome: str):
     pass

   def add_group_by(column: str) -> Generator:
     pass
     return self # so you can chain calls

   def add_selected_column(column: str) -> Generator:
     pass
     return self # so you can chain calls

   def add_condition(condition: str):
     pass

   def generate_sql() -> str:
     pass