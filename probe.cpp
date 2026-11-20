#include <windows.h>
#include <winhttp.h>

#include <atomic>
#include <cstdio>
#include <cstring>

struct TrialState {
    HANDLE sent = CreateEventW(nullptr, TRUE, FALSE, nullptr);
    HANDLE headers = CreateEventW(nullptr, TRUE, FALSE, nullptr);
    HANDLE shutdown = CreateEventW(nullptr, TRUE, FALSE, nullptr);
    HANDLE received = CreateEventW(nullptr, TRUE, FALSE, nullptr);
    HANDLE closing = CreateEventW(nullptr, TRUE, FALSE, nullptr);
    HANDLE failed = CreateEventW(nullptr, TRUE, FALSE, nullptr);
    std::atomic<DWORD> error{0};
    std::atomic<bool> shutdownCallback{false};
    std::atomic<HINTERNET> socketHandle{nullptr};
    std::atomic<unsigned> order{0};
    std::atomic<unsigned> shutdownOrder{0};
    std::atomic<unsigned> receiveOrder{0};

    ~TrialState() {
        CloseHandle(sent);
        CloseHandle(headers);
        CloseHandle(shutdown);
        CloseHandle(received);
        CloseHandle(closing);
        CloseHandle(failed);
    }
};

void CALLBACK OnStatus(HINTERNET handle, DWORD_PTR context, DWORD status, LPVOID info, DWORD) {
    auto* state = reinterpret_cast<TrialState*>(context);
    if (state == nullptr) return;
    switch (status) {
    case WINHTTP_CALLBACK_STATUS_SENDREQUEST_COMPLETE:
        SetEvent(state->sent);
        break;
    case WINHTTP_CALLBACK_STATUS_HEADERS_AVAILABLE:
        SetEvent(state->headers);
        break;
    case WINHTTP_CALLBACK_STATUS_SHUTDOWN_COMPLETE:
        state->shutdownOrder.store(++state->order);
        state->shutdownCallback.store(true);
        SetEvent(state->shutdown);
        break;
    case WINHTTP_CALLBACK_STATUS_READ_COMPLETE:
        state->receiveOrder.store(++state->order);
        SetEvent(state->received);
        break;
    case WINHTTP_CALLBACK_STATUS_HANDLE_CLOSING:
        if (handle == state->socketHandle.load()) SetEvent(state->closing);
        break;
    case WINHTTP_CALLBACK_STATUS_REQUEST_ERROR:
        if (info != nullptr) {
            state->error.store(reinterpret_cast<WINHTTP_ASYNC_RESULT*>(info)->dwError);
        }
        SetEvent(state->failed);
        break;
    }
}

bool WaitStage(HANDLE expected, TrialState& state, const char* stage) {
    HANDLE events[] = {expected, state.failed};
    DWORD result = WaitForMultipleObjects(2, events, FALSE, 10000);
    if (result == WAIT_OBJECT_0) return true;
    std::printf("stage=%s wait=%lu async_error=%lu\n", stage, result,
                state.error.load());
    return false;
}

bool OneTrial(const wchar_t* path, bool mutate) {
    TrialState state;
    HINTERNET session = nullptr, connection = nullptr, request = nullptr, socket = nullptr;
    bool ok = false;
    char receiveBuffer[4096]{};
    DWORD receiveResult = 0;

    session = WinHttpOpen(L"Inert buffer lifecycle check",
                          WINHTTP_ACCESS_TYPE_NO_PROXY, nullptr, nullptr,
                          WINHTTP_FLAG_ASYNC);
    if (!session) goto done;
    connection = WinHttpConnect(session, L"127.0.0.1", 18765, 0);
    if (!connection) goto done;
    request = WinHttpOpenRequest(connection, L"GET", path, nullptr, nullptr,
                                 nullptr, 0);
    if (!request) goto done;
    if (!WinHttpSetOption(request, WINHTTP_OPTION_UPGRADE_TO_WEB_SOCKET, nullptr, 0))
        goto done;
    if (WinHttpSetStatusCallback(request, OnStatus,
            WINHTTP_CALLBACK_FLAG_ALL_NOTIFICATIONS, 0) ==
        WINHTTP_INVALID_STATUS_CALLBACK) goto done;

    if (!WinHttpSendRequest(request, WINHTTP_NO_ADDITIONAL_HEADERS, 0,
                            nullptr, 0, 0,
                            reinterpret_cast<DWORD_PTR>(&state)) &&
        GetLastError() != ERROR_IO_PENDING) goto done;
    if (!WaitStage(state.sent, state, "send")) goto done;
    if (!WinHttpReceiveResponse(request, nullptr) &&
        GetLastError() != ERROR_IO_PENDING) goto done;
    if (!WaitStage(state.headers, state, "headers")) goto done;

    socket = WinHttpWebSocketCompleteUpgrade(request,
                                             reinterpret_cast<DWORD_PTR>(&state));
    if (!socket) goto done;
    state.socketHandle.store(socket);
    WinHttpCloseHandle(request);
    request = nullptr;
    if (WinHttpSetStatusCallback(socket, OnStatus,
            WINHTTP_CALLBACK_FLAG_ALL_NOTIFICATIONS, 0) ==
        WINHTTP_INVALID_STATUS_CALLBACK) goto done;
    ResetEvent(state.failed);

    receiveResult = WinHttpWebSocketReceive(socket, receiveBuffer,
                                            sizeof(receiveBuffer), nullptr,
                                            nullptr);
    std::printf("trial=%s receive_return=%lu\n",
                mutate ? "mutate" : "baseline", receiveResult);
    if (receiveResult != NO_ERROR && receiveResult != ERROR_IO_PENDING) goto done;

    {
        char reason[] = "AAAAAAAAAAAAAAAAAAAAAAAA";
        const DWORD length = static_cast<DWORD>(std::strlen(reason));
        DWORD result = WinHttpWebSocketShutdown(socket, 1000, reason, length);
        bool callbackAtReturn = state.shutdownCallback.load();
        if (mutate) {
            volatile char* p = reason;
            for (DWORD i = 0; i < length; ++i) p[i] = 'B';
        }
        std::printf("trial=%s shutdown_return=%lu callback_at_return=%d\n",
                    mutate ? "mutate" : "baseline", result,
                    callbackAtReturn ? 1 : 0);
        if (result != NO_ERROR && result != ERROR_IO_PENDING) goto done;
        if (!WaitStage(state.shutdown, state, "shutdown")) goto done;
        if (!WaitStage(state.received, state, "receive")) goto done;
        std::printf("trial=%s shutdown_order=%u receive_order=%u\n",
                    mutate ? "mutate" : "baseline",
                    state.shutdownOrder.load(), state.receiveOrder.load());
        Sleep(250);
    }
    ok = true;

done:
    if (!ok) std::printf("trial=%s setup_or_operation_error=%lu\n",
                         mutate ? "mutate" : "baseline", GetLastError());
    if (socket) {
        WinHttpCloseHandle(socket);
        if (WaitForSingleObject(state.closing, 10000) != WAIT_OBJECT_0) {
            std::printf("trial=%s closing_callback_missing\n",
                        mutate ? "mutate" : "baseline");
            ok = false;
        }
    }
    if (request) WinHttpCloseHandle(request);
    if (connection) WinHttpCloseHandle(connection);
    if (session) WinHttpCloseHandle(session);
    return ok;
}

int main() {
    if (!OneTrial(L"/baseline", false)) return 2;
    if (!OneTrial(L"/mutate", true)) return 3;
    return 0;
}
