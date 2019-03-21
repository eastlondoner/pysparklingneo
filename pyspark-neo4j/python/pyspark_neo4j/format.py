# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class ColumnSelector(object):
    def __init__(self, partition_key=False, primary_key=False, *columns):
        if sum([bool(partition_key), bool(primary_key), bool(columns)]) > 1:
            raise ValueError(
                "can't combine selection of partition_key "
                "and/or primary_key and/or columns")

        self.partition_key = partition_key
        self.primary_key = primary_key
        self.columns = columns

    @classmethod
    def none(cls):
        return ColumnSelector()

    @classmethod
    def partition_key(cls):
        return ColumnSelector(partition_key=True)

    @classmethod
    def primary_key(cls):
        return ColumnSelector(primary_key=True)

    @classmethod
    def some(cls, *columns):
        return ColumnSelector(columns)

    def __str__(self):
        s = '[column selection of: '
        if self.partition_key:
            s += 'partition_key'
        elif self.primary_key:
            s += 'primary_key'
        elif self.columns:
            s += ', '.join(c for c in self.columns)
        else:
            s += 'nothing'
        return s + ']'
