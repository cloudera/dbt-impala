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
    information: str = None
    
    def render(self):
        return super().render()
