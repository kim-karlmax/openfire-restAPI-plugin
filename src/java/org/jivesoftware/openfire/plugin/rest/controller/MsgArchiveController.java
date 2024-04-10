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

import org.jivesoftware.database.DbConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmpp.packet.JID;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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

    /** The Constant DELETE_OFCONVERSATION_BY_CONV_ID. */
    private static final String DELETE_OFCONVERSATION_BY_CONV_ID = "DELETE FROM ofConversation WHERE conversationID = ?";

    /** The Constant DELETE_OFCONPARTICIPANT_BY_CONV_ID. */
    private static final String DELETE_OFCONPARTICIPANT_BY_CONV_ID = "DELETE FROM ofConParticipant WHERE conversationID = ?";

    /** The Constant DELETE_OFMESSAGEARCHIVE_BY_CONV_ID. */
    private static final String DELETE_OFMESSAGEARCHIVE_BY_CONV_ID = "DELETE FROM ofMessageArchive WHERE conversationID = ?";



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
     * Get the number of conversations that archived to the user.
     *
     * @param jid the jid
     * @return The number of conversations.
     */
    public int getChatArchiveCountSingleUser(JID jid) {
        LOG.debug("invoked getChatArchiveCountSingleUser:" + jid.toBareJID());
        return getConversationIDsSingleUser(jid).size();
    }

    /**
     * Delete the all conversations that archived to the user.
     *
     * @param jid the jid
     * @return The number of deleted conversations.
     */
    public int deleteChatArchiveSingleUser(JID jid) {
        LOG.debug("invoked deleteChatArchiveSingleUser:" + jid.toBareJID());
        int count;
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
     * Get the number of conversations that archived between two users.
     *
     * @param jid1 the jid of first user
     * @param jid2 the jid of second user
     * @return the number of conversations that archived between two users.
     */
    public int getChatArchiveBetweenTwoUsers(JID jid1, JID jid2) {
        LOG.debug("invoked getChatArchiveBetweenTwoUsers:" + jid1.toBareJID() + "," + jid2.toBareJID());
        return getConversationIDsTwoUser(jid1, jid2).size();
    }

    /**
     * Delete the all conversations that archived between two users.
     *
     * @param jid1 the jid of first user
     * @param jid2 the jid of second user
     * @return The number of deleted archived conversations between two users.
     */
    public int deleteChatArchiveBetweenTwoUsers(JID jid1, JID jid2) {
        LOG.debug("invoked deleteChatArchiveBetweenTwoUsers:" + jid1.toBareJID() + "," + jid2.toBareJID());
        int count;
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

    /**
     * Gets a list of conversation IDs for one user among the conversations archived in the database.
     *
     * @param jid1 the jid of a user
     * @return The list of conversation IDs
     */
    private List<Integer> getConversationIDsSingleUser(JID jid1) {
        LOG.debug("invoked getConversationIDsSingleUser:" + jid1.toBareJID());

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<Integer> ids = new ArrayList<>(0);
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

    /**
     * Gets a list of conversation IDs between two users among the conversations archived in the database.
     *
     * @param jid1 the jid of first user
     * @param jid2 the jid of second user
     * @return The list of conversation IDs
     */
    private List<Integer> getConversationIDsTwoUser(JID jid1, JID jid2) {
        LOG.debug("invoked getConversationIDsTwoUser:" + jid1.toBareJID() + "," + jid2.toBareJID());

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<Integer> ids = new ArrayList<>(0);
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

    /**
     * Delete archived conversations by conversation ID in the database.
     * Deletes archived conversations from the ofConversation, ofConParticipant, and ofMessageArchive tables in the database.
     *
     * @param conversationIDs the list of conversation IDs
     */
    private void deleteConversationsByID(List<Integer> conversationIDs) {
        LOG.debug("invoked deleteConversationsByID:" + conversationIDs.toString());

        Connection con = null;
        PreparedStatement pstmt1 = null;
        PreparedStatement pstmt2 = null;
        PreparedStatement pstmt3 = null;

        try {
            con = DbConnectionManager.getConnection();
            pstmt1 = con.prepareStatement(DELETE_OFCONVERSATION_BY_CONV_ID);
            pstmt2 = con.prepareStatement(DELETE_OFCONPARTICIPANT_BY_CONV_ID);
            pstmt3 = con.prepareStatement(DELETE_OFMESSAGEARCHIVE_BY_CONV_ID);

            for (Integer id: conversationIDs) {
                LOG.warn("Deleting conversation: " + id);
                pstmt1.setInt(1, id);
                pstmt1.execute();
                pstmt2.setInt(1, id);
                pstmt2.execute();
                pstmt3.setInt(1, id);
                pstmt3.execute();
            }
        } catch (SQLException sqle) {
            LOG.error(sqle.getMessage(), sqle);
        } finally {
            DbConnectionManager.closeConnection(pstmt1, con);
            DbConnectionManager.closeConnection(pstmt2, con);
            DbConnectionManager.closeConnection(pstmt3, con);
        }
    }
}
