import logging
from airflow.utils.decorators import apply_defaults
from airflow.operators.bash import BashOperator
from plugins.utils.constants import DBT_PROJECT_DIR, DBT_PROFILES_DIR

logger = logging.getLogger(__name__)


class DbtOperator(BashOperator):
    """
    A simple wrapper over BashOperator to simplify executing DBT commands.
    See BashOperator for all operator parameters.

    :param dbt_command: The dbt command to execute

    """

    @apply_defaults
    def __init__(self, dbt_command, *args, **kwargs):
        super(DbtOperator, self).__init__(
            bash_command=f"{dbt_command} --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}",
            *args,
            **kwargs,
        )
        self.dbt_command = dbt_command