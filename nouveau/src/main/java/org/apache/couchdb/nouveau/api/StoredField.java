//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.couchdb.nouveau.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotNull;

public final class StoredField extends Field {

    @NotNull
    private final Object value;

    private final boolean encoded;

    public StoredField(@JsonProperty("name") final String name, @JsonProperty("value") final String value) {
        super(name);
        this.value = value;
        this.encoded = false;
    }

    public StoredField(@JsonProperty("name") final String name, @JsonProperty("value") final double value) {
        super(name);
        this.value = value;
        this.encoded = false;
    }

    public StoredField(@JsonProperty("name") final String name, @JsonProperty("value") final byte[] value) {
        super(name);
        this.value = value;
        this.encoded = true;
    }

    @JsonProperty
    public Object getValue() {
        return value;
    }

    @JsonProperty
    public boolean isEncoded() {
        return encoded;
    }

    @Override
    public String toString() {
        return "StoredField [value=" + value + ", encoded=" + encoded + "]";
    }

}