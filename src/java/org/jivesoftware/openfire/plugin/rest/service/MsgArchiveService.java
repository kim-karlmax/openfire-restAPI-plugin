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

package org.jivesoftware.openfire.plugin.rest.service;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.jivesoftware.openfire.plugin.rest.controller.MsgArchiveController;
import org.jivesoftware.openfire.plugin.rest.entity.ConversationArchiveEntity;
import org.jivesoftware.openfire.plugin.rest.entity.MsgArchiveEntity;
import org.jivesoftware.openfire.plugin.rest.exceptions.ExceptionType;
import org.jivesoftware.openfire.plugin.rest.exceptions.ServiceException;
import org.xmpp.packet.JID;

import javax.annotation.PostConstruct;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;

@Path("restapi/v1/archive")
@Tag(name = "Message Archive", description = "Server-sided storage of chat messages.")
public class MsgArchiveService {

    private MsgArchiveController archive;

    @PostConstruct
    public void init() {
        archive = MsgArchiveController.getInstance();
    }

    @GET
    @Path("/messages/unread/{jid}")
    @Operation( summary = "Unread message count",
        description = "Gets a count of messages that haven't been delivered to the user yet.",
        responses = {
            @ApiResponse(responseCode = "200", description = "A message count", content = @Content(schema = @Schema(implementation = MsgArchiveEntity.class)))
        })
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public MsgArchiveEntity getUnReadMessagesCount(@Parameter(description = "The (bare) JID of the user for which the unread message count needs to be fetched.", example = "john@example.org", required = true) @PathParam("jid") String jidStr)
        throws ServiceException
    {
        JID jid = new JID(jidStr);
        int msgCount = archive.getUnReadMessagesCount(jid);
        return new MsgArchiveEntity(jidStr, msgCount);
    }

    @GET
    @Path("/user/{jid}")
    @Operation( summary = "Get chat archive count for a single user",
        description = "Get chat archive count for a single user.",
        responses = {
            @ApiResponse(responseCode = "200", description = "count of archived messages for a single user", content = @Content(schema = @Schema(implementation = MsgArchiveEntity.class)))
        })
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public MsgArchiveEntity getChatArchiveCountSingleUser(@Parameter(description = "The (bare) JID of the user whose archived messages should be retrieved.", example = "john@example.org", required = true) @PathParam("jid") String jidStr)
        throws ServiceException
    {
        JID jid = new JID(jidStr);
        int msgCount = archive.getChatArchiveCountSingleUser(jid);
        return new MsgArchiveEntity(jidStr, msgCount);
    }

    @DELETE
    @Path("/user/{jid}")
    @Operation( summary = "Delete a single user's chat archive",
        description = "Delete a single user's chat archive.",
        responses = {
            @ApiResponse(responseCode = "200", description = "Delete a single user's chat archive", content = @Content(schema = @Schema(implementation = MsgArchiveEntity.class)))
        })
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public MsgArchiveEntity deleteChatArchiveSingleUser(@Parameter(description = "The (bare) JID of the user whose archived messages need to be cleared.", example = "john@example.org", required = true) @PathParam("jid") String jidStr)
        throws ServiceException
    {
        JID jid = new JID(jidStr);
        int deleteCount;
        try {
            deleteCount = archive.deleteChatArchiveSingleUser(jid);
        } catch (Exception e) {
            throw new ServiceException("Could not delete the user archived messages", jid.toBareJID(),
                ExceptionType.SHARED_GROUP_EXCEPTION, Response.Status.BAD_REQUEST, e);
        }
        return new MsgArchiveEntity(jidStr, deleteCount);
    }

    @GET
    @Path("/conversation/{jid1}/{jid2}")
    @Operation( summary = "Get count of archived conversations between two users",
        description = "Get count of archived conversations between two users.",
        responses = {
            @ApiResponse(responseCode = "200", description = "count of archived conversations between two users", content = @Content(schema = @Schema(implementation = ConversationArchiveEntity.class)))
        })
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public ConversationArchiveEntity getChatArchiveBetweenTwoUsers(
        @Parameter(description = "The (bare) JIDs of the first of the two users whose archived messages you need to retrieve.", example = "John@example.org", required = true) @PathParam("jid1") String jid1Str,
        @Parameter(description = "The (bare) JIDs of the second of the two users whose archived messages you need to retrieve.", example = "Oliver@example.org", required = true) @PathParam("jid2") String jid2Str)
        throws ServiceException
    {
        JID jid1 = new JID(jid1Str);
        JID jid2 = new JID(jid2Str);
        int msgCount = archive.getChatArchiveBetweenTwoUsers(jid1, jid2);
        return new ConversationArchiveEntity(jid1Str, jid2Str, msgCount);
    }

    @DELETE
    @Path("/conversation/{jid1}/{jid2}")
    @Operation( summary = "Delete archived conversations between two users",
        description = "Delete archived conversations between two users.",
        responses = {
            @ApiResponse(responseCode = "200", description = "Delete archived conversations between two users", content = @Content(schema = @Schema(implementation = ConversationArchiveEntity.class)))
        })
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public ConversationArchiveEntity deleteChatArchiveBetweenTwoUsers(
        @Parameter(description = "The (bare) JIDs of the first of two users whose archived messages need to be cleared.", example = "John@example.org", required = true) @PathParam("jid1") String jid1Str,
        @Parameter(description = "The (bare) JIDs of the second of two users whose archived messages need to be cleared.", example = "Oliver@example.org", required = true) @PathParam("jid2") String jid2Str)
        throws ServiceException
    {
        JID jid1 = new JID(jid1Str);
        JID jid2 = new JID(jid2Str);
        int deleteCount = 0;
        try {
            deleteCount = archive.deleteChatArchiveBetweenTwoUsers(jid1, jid2);
        } catch (Exception e) {
            throw new ServiceException("Could not delete the conversations archived messages", jid1Str + " + " + jid2Str,
                ExceptionType.SHARED_GROUP_EXCEPTION, Response.Status.BAD_REQUEST, e);
        }
        return new ConversationArchiveEntity(jid1Str, jid2Str, deleteCount);
    }
}
