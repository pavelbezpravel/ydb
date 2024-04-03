#include <ydb/core/etcd/kv/events.h>
#include <ydb/core/etcd/kv/kv.h>
#include <ydb/core/etcd/kv/proto.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/kqp/common/simple/services.h>

#include <library/cpp/testing/unittest/registar.h>

namespace {

void AssertEquals(const NYdb::NEtcd::TPutResponse& /* expected */, const NYdb::NEtcd::TPutResponse& /* actual */) {
    UNIT_ASSERT(true);
}

class TGrabActor: public NActors::TActor<TGrabActor> {
public:
    TGrabActor(std::weak_ptr<NActors::TTestBasicRuntime> runtime)
        : TActor(&TGrabActor::StateFunc)
        , WeakRuntime(runtime)
    {
    }

    STFUNC(StateFunc) {
        TGuard<TMutex> lock(Mutex);
        if (!Futures.empty()) {
            auto front = Futures.front();
            Futures.pop_front();
            front.SetValue(ev);
            return;
        }
        Inputs.push_back(ev);
    }

    NThreading::TFuture<TAutoPtr<NActors::IEventHandle>> WaitRequest() {
        TGuard<TMutex> lock(Mutex);
        if (!Inputs.empty()) {
            auto front = Inputs.front();
            Inputs.pop_front();
            return NThreading::MakeFuture(front);
        }
        Futures.push_back(NThreading::NewPromise<TAutoPtr<NActors::IEventHandle>>());
        return Futures.back();
    }

    TAutoPtr<NActors::IEventHandle> GetRequest() {
        auto runtime = WeakRuntime.lock();
        if (!runtime) {
            throw std::runtime_error("runtime was already cleared");
        }
        auto future = WaitRequest();
        while (!future.HasValue()) {
            runtime->DispatchEvents({}, TDuration::MilliSeconds(1));
        }
        return future.GetValue();
    }

private:
    std::weak_ptr<NActors::TTestBasicRuntime> WeakRuntime;
    std::deque<NThreading::TPromise<TAutoPtr<NActors::IEventHandle>>> Futures;
    std::deque<TAutoPtr<NActors::IEventHandle>> Inputs;
    TMutex Mutex;
};

struct TTestRuntime {
    std::shared_ptr<NActors::TTestBasicRuntime> Runtime;
    TGrabActor* Sender;
    NActors::TActorId KvPutActor;

    TTestRuntime()
        : Runtime(std::make_shared<NActors::TTestBasicRuntime>())
        , Sender(new TGrabActor(Runtime))
    {
        Runtime->AddLocalService(
            NKikimr::NKqp::MakeKqpProxyID(0),
            NActors::TActorSetupCmd(Sender, NActors::TMailboxType::Simple, 0),
            0);

        Runtime->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_TRACE);

        NKikimr::SetupTabletServices(*Runtime);
    }

    void Send(NYdb::NEtcd::TPutRequest&& putRequest) {
        KvPutActor = Runtime->Register(CreateKvActor(NKikimrServices::EServiceKikimr::KQP_PROXY, "SessionId", "", std::move(putRequest)));
    }

    template <typename TEvent, typename TResponse>
    void AssertEvent(const TResponse& expected) {
        TAutoPtr<NActors::IEventHandle> eventHolder = Sender->GetRequest();
        UNIT_ASSERT(eventHolder.Get() != nullptr);

        TEvent* response = eventHolder.Get()->Get<TEvent>();
        UNIT_ASSERT(response != nullptr);
        UNIT_ASSERT(response->Status == Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(!response->Issues);

        const TResponse& actual = response->Response;
        AssertEquals(expected, actual);
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TTestKvPut) {
    Y_UNIT_TEST(Naive) {
        TTestRuntime runtime;

        runtime.Send(NYdb::NEtcd::TPutRequest{
            .Kvs = {
                {"key", "value"},
            },
            .PrevKv = true,
        });

        runtime.AssertEvent<NYdb::NEtcd::TEvEtcdKv::TEvPutResponse>(NYdb::NEtcd::TPutResponse{
            .PrevKvs = {},
        });
    }
}
