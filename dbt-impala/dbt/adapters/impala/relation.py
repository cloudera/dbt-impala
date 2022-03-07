from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.exceptions import RuntimeException


@dataclass
class ImpalaQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False

@dataclass
class ImpalaIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class ImpalaRelation(BaseRelation):
    quote_policy: ImpalaQuotePolicy = ImpalaQuotePolicy()
    include_policy: ImpalaIncludePolicy = ImpalaIncludePolicy()
    quote_character: str = None
    
    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise RuntimeException('Cannot set database in Impala!')

    def render(self):
        
        # if self.include_policy.database and self.include_policy.schema:
        #     raise RuntimeException(
        #         'Got a Impala relation with schema and database set to '
        #         'include, but only one can be set'
        #     )
            
        return super().render()
