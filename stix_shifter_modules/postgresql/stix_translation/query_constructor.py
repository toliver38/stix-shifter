from stix_shifter_utils.stix_translation.src.patterns.pattern_objects import ObservationExpression, ComparisonExpression, \
    ComparisonExpressionOperators, ComparisonComparators, Pattern, StartStopQualifier,\
    CombinedComparisonExpression, CombinedObservationExpression, ObservationOperators
from stix_shifter_utils.stix_translation.src.utils.transformers import TimestampToMilliseconds
from stix_shifter_utils.stix_translation.src.json_to_stix import observable
import logging
import re

# Source and destination reference mapping for IP and MAC addresses.
# Change the keys to match the data source fields. The value array indicates the possible data type that can come into from field.
REFERENCE_DATA_TYPES = {"source_ipaddr": ["ipv4", "ipv4_cidr", "ipv6", "ipv6_cidr"],
                        "dest_ipaddr": ["ipv4", "ipv4_cidr"],
                        }

TIMESTAMP_STIX_PROPERTIES = ["created", "modified", "accessed", "ctime", "mtime", "atime", "created_time", "modifed_time"]

logger = logging.getLogger(__name__)


class QueryStringPatternTranslator:

    def __init__(self, pattern: Pattern, data_model_mapper):
        self.dmm = data_model_mapper
        self.comparator_lookup = self.dmm.map_comparator()
        self.pattern = pattern
        self.translated = self.parse_expression(pattern)

    @staticmethod
    def _format_set(values) -> str:
        gen = values.element_iterator()
        return "({})".format(' OR '.join([QueryStringPatternTranslator._escape_value(value) for value in gen]))

    @staticmethod
    def _format_match(value) -> str:
        raw = QueryStringPatternTranslator._escape_value(value)
        if raw[0] == "^":
            raw = raw[1:]
        else:
            raw = ".*" + raw
        if raw[-1] == "$":
            raw = raw[0:-1]
        else:
            raw = raw + ".*"
        return "'{}'".format(raw)

    @staticmethod
    def _format_equality(value) -> str:
        return "'{}'".format(value)

    @staticmethod
    def _format_like(value) -> str:
        value = "'%{value}%'".format(value=value)
        return QueryStringPatternTranslator._escape_value(value)

    @classmethod
    def _format_start_stop_qualifier(cls, expression, qualifier) -> str:
        """Convert a STIX start stop qualifier into a query string.

        The sample PostgreSQL schema included in this connector defines a timerange with a start and stop value
        based on the entry_time field.
        """
        transformer = TimestampToMilliseconds()
        qualifier_split = qualifier.split("'")
        start = transformer.transform(qualifier_split[1])
        stop = transformer.transform(qualifier_split[3])
        qualified_query = "{} AND (entry_time >= {} AND entry_time <= {})".format(expression, start, stop)
        return qualified_query

    @classmethod
    def _format_timestamp(cls, value):
        transformer = TimestampToMilliseconds()
        value = re.sub("'", "", value)
        return transformer.transform(value)

    @staticmethod
    def _escape_value(value, comparator=None) -> str:
        if isinstance(value, str):
            return '{}'.format(value.replace('\\', '\\\\').replace('\'', '\\\'').replace('\"', '\\"').replace('(', '\\(').replace(')', '\\)'))
        else:
            return value

    @staticmethod
    def _negate_comparison(comparison_string):
        return "NOT ({})".format(comparison_string)

    @staticmethod
    def _check_value_type(value):
        value = str(value)
        for key, pattern in observable.REGEX.items():
            if key != 'date' and bool(re.search(pattern, value)):
                return key
        return None

    @staticmethod
    def _parse_reference(stix_field, value_type, mapped_field, value, comparator):
        if value_type not in REFERENCE_DATA_TYPES["{}".format(mapped_field)]:
            return None
        else:
            return "{} {} {}".format(mapped_field, comparator, value)

    @staticmethod
    def _parse_mapped_fields(expression, value, comparator, stix_field, mapped_fields_array):
        if stix_field in TIMESTAMP_STIX_PROPERTIES:
            value = QueryStringPatternTranslator._format_timestamp(value)
        comparison_string = ""
        is_reference_value = QueryStringPatternTranslator._is_reference_value(stix_field)
        value_type = QueryStringPatternTranslator._check_value_type(expression.value) if is_reference_value else None
        mapped_fields_count = 1 if is_reference_value else len(mapped_fields_array)

        for mapped_field in mapped_fields_array:
            if is_reference_value:
                parsed_reference = QueryStringPatternTranslator._parse_reference(stix_field, value_type, mapped_field, value, comparator)
                if not parsed_reference:
                    continue
                comparison_string += parsed_reference
            else:
                comparison_string += "{} {} {}".format(mapped_field, comparator, value)

            if mapped_fields_count > 1:
                comparison_string += " OR "
                mapped_fields_count -= 1
        return comparison_string

    @staticmethod
    def _is_reference_value(stix_field):
        return stix_field == 'src_ref.value' or stix_field == 'dst_ref.value'

    @staticmethod
    def _lookup_comparison_operator(expression_operator, comparator_lookup):
        if str(expression_operator) not in comparator_lookup:
            raise NotImplementedError("Comparison operator {} unsupported for PostgreSQL connector".format(expression_operator.name))
        return comparator_lookup[str(expression_operator)]

    def _parse_expression(self, expression, qualifier=None) -> str:
        if isinstance(expression, ComparisonExpression):  # Base Case
            # Resolve STIX Object Path to a field in the target Data Model
            stix_object, stix_field = expression.object_path.split(':')
            # Multiple data source fields may map to the same STIX Object
            mapped_fields_array = self.dmm.map_field(stix_object, stix_field)
            # Resolve the comparison symbol to use in the query string
            comparator = QueryStringPatternTranslator._lookup_comparison_operator(expression.comparator, self.comparator_lookup)

            if stix_field in {'start', 'end'}:
                transformer = TimestampToMilliseconds()
                expression.value = transformer.transform(expression.value)

            # Some values are formatted differently based on how they're being compared
            if expression.comparator == ComparisonComparators.Matches:
                value = QueryStringPatternTranslator._format_match(expression.value)
            elif expression.comparator == ComparisonComparators.In:
                value = QueryStringPatternTranslator._format_set(expression.value)
            elif expression.comparator in {ComparisonComparators.Equal, ComparisonComparators.NotEqual}:
                value = QueryStringPatternTranslator._format_equality(expression.value)
            elif expression.comparator == ComparisonComparators.Like:
                value = QueryStringPatternTranslator._format_like(expression.value)
            else:
                value = QueryStringPatternTranslator._escape_value(expression.value)

            comparison_string = QueryStringPatternTranslator._parse_mapped_fields(expression, value, comparator, stix_field, mapped_fields_array)
            if len(mapped_fields_array) > 1 and not QueryStringPatternTranslator._is_reference_value(stix_field):
                grouped_comparison_string = "({})".format(comparison_string)
                comparison_string = grouped_comparison_string

            if expression.negated:
                comparison_string = QueryStringPatternTranslator._negate_comparison(comparison_string)
            if qualifier:
                comparison_string = QueryStringPatternTranslator._format_start_stop_qualifier(comparison_string, qualifier)
                return comparison_string
            else:
                return "{}".format(comparison_string)

        elif isinstance(expression, CombinedComparisonExpression):
            operator = QueryStringPatternTranslator._lookup_comparison_operator(expression.operator, self.comparator_lookup)
            expression_01 = self._parse_expression(expression.expr1)
            expression_02 = self._parse_expression(expression.expr2)
            if not expression_01 or not expression_02:
                return ''
            if isinstance(expression.expr1, CombinedComparisonExpression):
                expression_01 = "{}".format(expression_01)
            if isinstance(expression.expr2, CombinedComparisonExpression):
                expression_02 = "{}".format(expression_02)
            if operator == 'AND':
                query_string = "({} {} {})".format(expression_01, operator, expression_02)
            else:
                query_string = "{} {} {}".format(expression_01, operator, expression_02)
            if qualifier:
                query_string = QueryStringPatternTranslator._format_start_stop_qualifier(query_string, qualifier)
                return query_string
            else:
                return "{}".format(query_string)
        elif isinstance(expression, ObservationExpression):
            return self._parse_expression(expression.comparison_expression, qualifier)
        elif hasattr(expression, 'qualifier') and hasattr(expression, 'observation_expression'):
            if isinstance(expression.observation_expression, CombinedObservationExpression):
                operator = QueryStringPatternTranslator._lookup_comparison_operator(expression.observation_expression.operator, self.comparator_lookup)
                expression_01 = self._parse_expression(expression.observation_expression.expr1)
                expression_02 = self._parse_expression(expression.observation_expression.expr2, expression.qualifier)
                return "{} {} {}".format(expression_01, operator, expression_02)
            else:
                return self._parse_expression(expression.observation_expression.comparison_expression, expression.qualifier)
        elif isinstance(expression, CombinedObservationExpression):
            operator = QueryStringPatternTranslator._lookup_comparison_operator(expression.operator, self.comparator_lookup)
            expression_01 = self._parse
