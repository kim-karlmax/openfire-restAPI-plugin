/*
 * Copyright (c) 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jivesoftware.openfire.plugin.rest.entity;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * The Class ConversationArchiveEntity.
 */
@XmlRootElement(name = "conversation")
public class ConversationArchiveEntity {

    /**
     * users in conversation
     */
    @XmlElement(name = "jid1")
    String jid1;

    @XmlElement(name = "jid2")
    String jid2;

    /**
     * conversation count between users
     */
    @XmlElement(name = "count")
    int count;

    public ConversationArchiveEntity() {
    }

    public ConversationArchiveEntity(String jid1, String jid2, int count) {
        this.jid1 = jid1;
        this.jid2 = jid2;
        this.count = count;
    }
}
