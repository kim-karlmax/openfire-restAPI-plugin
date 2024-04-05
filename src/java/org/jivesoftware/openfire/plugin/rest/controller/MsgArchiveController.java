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

package org.jivesoftware.openfire.plugin.rest.controller;

import com.sun.org.apache.xalan.internal.xsltc.util.IntegerArray;
import org.jivesoftware.database.DbConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmpp.packet.JID;
import sun.jvm.hotspot.utilities.IntArray;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The Class MsgArchiveController.
 */
public class MsgArchiveController {

    /** The Constant LOG. */
    private static final Logger LOG = LoggerFactory.getLogger(MsgArchiveController.class);

    /** The Constant INSTANCE. */
    public static final MsgArchiveController INSTANCE = new MsgArchiveController();

    /** The Constant USER_MESSAGE_COUNT. */
    private static final String USER_MESSAGE_COUNT = "select COUNT(1) from ofMessageArchive a " +
            "join ofPresence p on (a.sentDate > p.offlineDate) " +
            "WHERE a.toJID = ? AND p.username = ?";

    /** The Constant CONVERSATION_IDS_SINGLE_USER. */
    private static final String CONVERSATION_IDS_SINGLE_USER = "SELECT conversationID FROM ofMessageArchive " +
        "WHERE (toJID = ? OR fromJID = ?) GROUP BY conversationID";

    /** The Constant CONVERSATION_IDS_TWO_USER. */
    private static final String CONVERSATION_IDS_TWO_USER = "SELECT conversationID FROM ofMessageArchive " +
        "WHERE (toJID = ? AND fromJID = ?) OR (toJID = ? AND fromJID = ?) GROUP BY conversationID";

//    /** The Constant DELETE_OFCONVERSATION_BY_CONV_ID. */
//    private static final String DELETE_OFCONVERSATION_BY_CONV_ID = "DELETE FROM ofConversation " +
//        "WHERE conversationID = ?";
//
//    /** The Constant DELETE_OFCONPARTICIPANT_BY_CONV_ID. */
//    private static final String DELETE_OFCONPARTICIPANT_BY_CONV_ID = "DELETE FROM ofConParticipant " +
//        "WHERE conversationID = ?";
//
//    /** The Constant DELETE_OFMESSAGEARCHIVE_BY_CONV_ID. */
//    private static final String DELETE_OFMESSAGEARCHIVE_BY_CONV_ID = "DELETE FROM ofMessageArchive " +
//        "WHERE conversationID = ?";

//    /** The Constant DELETE_OFCONVERSATION_BY_CONV_ID. */
//    private static final String DELETE_OFCONVERSATION_BY_CONV_ID = "DELETE FROM ofConversation WHERE conversationID IN (?)";
//
//    /** The Constant DELETE_OFCONPARTICIPANT_BY_CONV_ID. */
//    private static final String DELETE_OFCONPARTICIPANT_BY_CONV_ID = "DELETE FROM ofConParticipant WHERE conversationID IN (?)";
//
//    /** The Constant DELETE_OFMESSAGEARCHIVE_BY_CONV_ID. */
//    private static final String DELETE_OFMESSAGEARCHIVE_BY_CONV_ID = "DELETE FROM ofMessageArchive WHERE conversationID IN (?)";

    /** The Constant DELETE_OFCONVERSATION_BY_CONV_ID. */
    private static final String DELETE_OFCONVERSATION_BY_CONV_ID = "DELETE FROM ofConversation WHERE conversationID IN (%s)";

    /** The Constant DELETE_OFCONPARTICIPANT_BY_CONV_ID. */
    private static final String DELETE_OFCONPARTICIPANT_BY_CONV_ID = "DELETE FROM ofConParticipant WHERE conversationID IN (%s)";

    /** The Constant DELETE_OFMESSAGEARCHIVE_BY_CONV_ID. */
    private static final String DELETE_OFMESSAGEARCHIVE_BY_CONV_ID = "DELETE FROM ofMessageArchive WHERE conversationID IN (%s)";



    /**
     * Gets the single instance of MsgArchiveController.
     *
     * @return single instance of MsgArchiveController
     */
    public static MsgArchiveController getInstance() {
        return INSTANCE;
    }

    /**
     * The Constructor.
     */
    private MsgArchiveController() {
    }

    /**
     * Returns the total number of messages that haven't been delivered to the user.
     *
     * @param jid the jid
     * @return the total number of user unread messages.
     */
    public int getUnReadMessagesCount(JID jid) {
        int messageCount = 0;
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = DbConnectionManager.getConnection();
            pstmt = con.prepareStatement(USER_MESSAGE_COUNT);
            pstmt.setString(1, jid.toBareJID());
            pstmt.setString(2, jid.getNode());
            rs = pstmt.executeQuery();
            if (rs.next()) {
                messageCount = rs.getInt(1);
            }
        } catch (SQLException sqle) {
            LOG.error(sqle.getMessage(), sqle);
        } finally {
            DbConnectionManager.closeConnection(rs, pstmt, con);
        }
        return messageCount;
    }

    /**
     * Get the number of messages that archived to the user.
     *
     * @param jid the jid
     * @return The number of archived messages.
     */
    public int getChatArchiveCountSingleUser(JID jid) {
        int count = 0;
        List<Integer> convIDs = getConversationIDsSingleUser(jid);
        count = convIDs.size();
        return count;
    }

    /**
     * Delete the all messages that archived to the user.
     *
     * @param jid the jid
     * @return The number of deleted archived messages.
     */
    public int deleteChatArchiveSingleUser(JID jid) {
        int count = 0;
        List<Integer> convIDs = getConversationIDsSingleUser(jid);
        count = convIDs.size();
        if (count > 0) {
            deleteConversationsByID(convIDs);
            LOG.warn("delete archived messages for user:" + jid.toBareJID() + ", size:" + count);
        } else {
            LOG.warn("no archived messages for user:" + jid.toBareJID());
        }
        return count;
    }


    /**
     * Get the number of messages that archived between two users.
     *
     * @param jid1 the jid of first user
     * @param jid2 the jid of second user
     * @return the number of messages that archived between two users.
     */
    public int getChatArchiveBetweenTwoUsers(JID jid1, JID jid2) {
        int count = 0;
        List<Integer> convIDs = getConversationIDsTwoUser(jid1, jid2);
        count = convIDs.size();
        return count;
    }

    /**
     * Delete the all messages that archived between two users.
     *
     * @param jid1 the jid of first user
     * @param jid2 the jid of second user
     * @return The number of deleted archived messages between two users.
     */
    public int deleteChatArchiveBetweenTwoUsers(JID jid1, JID jid2) {
        int count = 0;
        List<Integer> convIDs = getConversationIDsTwoUser(jid1, jid2);
        count = convIDs.size();
        if (count > 0) {
            deleteConversationsByID(convIDs);
            LOG.warn("delete archived conversation between " + jid1.toBareJID() + ", " + jid2.toBareJID() + ", size:" + count);
        } else {
            LOG.warn("no archived conversation between " + jid1.toBareJID() + ", " + jid2.toBareJID());
        }
        return count;
    }

    private List<Integer> getConversationIDsSingleUser(JID jid1) {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<Integer> ids = new ArrayList<Integer>(0);
        try {
            con = DbConnectionManager.getConnection();
            pstmt = con.prepareStatement(CONVERSATION_IDS_SINGLE_USER);
            pstmt.setString(1, jid1.toBareJID());
            pstmt.setString(2, jid1.toBareJID());
            rs = pstmt.executeQuery();
            while (rs.next()) {
                int id = rs.getInt(1);
                ids.add(id);
            }
        } catch (SQLException sqle) {
            LOG.error(sqle.getMessage(), sqle);
        } finally {
            DbConnectionManager.closeConnection(rs, pstmt, con);
        }
        return ids;
    }

    private List<Integer> getConversationIDsTwoUser(JID jid1, JID jid2) {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<Integer> ids = new ArrayList<Integer>(0);
        try {
            con = DbConnectionManager.getConnection();
            pstmt = con.prepareStatement(CONVERSATION_IDS_TWO_USER);
            pstmt.setString(1, jid1.toBareJID());
            pstmt.setString(2, jid2.toBareJID());
            pstmt.setString(3, jid2.toBareJID());
            pstmt.setString(4, jid1.toBareJID());
            rs = pstmt.executeQuery();
            while (rs.next()) {
                int id = rs.getInt(1);
                ids.add(id);
            }
        } catch (SQLException sqle) {
            LOG.error(sqle.getMessage(), sqle);
        } finally {
            DbConnectionManager.closeConnection(rs, pstmt, con);
        }
        return ids;
    }

    private void deleteConversationsByID(List<Integer> conversationIDs) {
        Connection con = null;
        PreparedStatement pstmt = null;

        try {
            con = DbConnectionManager.getConnection();


            String sql = makeArraySQLQuery(DELETE_OFCONVERSATION_BY_CONV_ID, conversationIDs);
            pstmt = con.prepareStatement(sql);
            pstmt.executeUpdate();

            sql = makeArraySQLQuery(DELETE_OFCONPARTICIPANT_BY_CONV_ID, conversationIDs);
            pstmt = con.prepareStatement(sql);
            pstmt.executeUpdate();

            sql = makeArraySQLQuery(DELETE_OFMESSAGEARCHIVE_BY_CONV_ID, conversationIDs);
            pstmt = con.prepareStatement(sql);
            pstmt.executeUpdate();
        } catch (SQLException sqle) {
            LOG.error(sqle.getMessage(), sqle);
        } finally {
            DbConnectionManager.closeConnection(null, pstmt, con);
        }
    }

    private String makeArraySQLQuery(String baseQuery, List<Integer> conversationIDs) {
        return String.format(baseQuery, conversationIDs.stream()
            .map(String::valueOf).collect(Collectors.joining(", ")));
    }
}
