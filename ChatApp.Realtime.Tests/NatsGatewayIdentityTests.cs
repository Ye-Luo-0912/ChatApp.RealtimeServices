using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;

namespace ChatApp.Realtime.Tests;

public sealed class NatsGatewayIdentityTests
{
    [Fact]
    public void ValidateIncomingSender_WhenRequired_RejectsMissingIdentity()
    {
        var error = NatsGatewayIdentity.ValidateIncomingSender(
            requireGatewayIdentity: true,
            trustedUserId: null,
            trustedSessionId: null,
            payloadSenderUserId: 10,
            payloadSenderSessionId: "s1");

        Assert.Equal("missing_gateway_identity", error);
    }

    [Fact]
    public void ValidateIncomingSender_WhenRequired_RejectsMismatchedUser()
    {
        var error = NatsGatewayIdentity.ValidateIncomingSender(
            requireGatewayIdentity: true,
            trustedUserId: 99,
            trustedSessionId: "s1",
            payloadSenderUserId: 10,
            payloadSenderSessionId: "s1");

        Assert.Equal("sender_identity_mismatch", error);
    }

    [Fact]
    public void ValidateIncomingSender_WhenRequired_RejectsMismatchedSession()
    {
        var error = NatsGatewayIdentity.ValidateIncomingSender(
            requireGatewayIdentity: true,
            trustedUserId: 10,
            trustedSessionId: "trusted",
            payloadSenderUserId: 10,
            payloadSenderSessionId: "forged");

        Assert.Equal("session_identity_mismatch", error);
    }

    [Fact]
    public void ValidateIncomingSender_WhenRequired_AcceptsMatchingIdentity()
    {
        var error = NatsGatewayIdentity.ValidateIncomingSender(
            requireGatewayIdentity: true,
            trustedUserId: 10,
            trustedSessionId: "s1",
            payloadSenderUserId: 10,
            payloadSenderSessionId: "s1");

        Assert.Null(error);
    }

    [Fact]
    public void ValidateIncomingSender_WhenDisabled_AllowsMissingIdentity()
    {
        var error = NatsGatewayIdentity.ValidateIncomingSender(
            requireGatewayIdentity: false,
            trustedUserId: null,
            trustedSessionId: null,
            payloadSenderUserId: 10,
            payloadSenderSessionId: "s1");

        Assert.Null(error);
    }

    [Fact]
    public void ValidateHistoryUser_WhenRequired_RejectsMismatch()
    {
        var error = NatsGatewayIdentity.ValidateHistoryUser(
            requireGatewayIdentity: true,
            trustedUserId: 7,
            payloadUserId: 8);

        Assert.Equal("history_user_identity_mismatch", error);
    }
}
