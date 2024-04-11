#include <ydb/core/etcd/kv/events.h>

#include <ydb/core/etcd/revision/events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NYdb::NEtcd {

class TEtcdService : public NActors::TActorBootstrapped<TEtcdService> {
public:
    void Bootstrap() {
        this->Become(&TEtcdService::StateProxy);
    }

private:
    STRICT_STFUNC(StateProxy,
        hFunc(TEvEtcdRevision::TEvCreateTableRequest, Handle)
        hFunc(TEvEtcdRevision::TEvRevisionResponse, Handle)

        hFunc(TEvEtcdKV::TEvCreateTableRequest, Handle)
        hFunc(TEvEtcdKV::TEvCreateTableResponse, Handle)

        hFunc(TEvEtcdKV::TEvRangeRequest, Handle)
        hFunc(TEvEtcdKV::TEvRangeResponse, Handle)

        hFunc(TEvEtcdKV::TEvPutRequest, Handle)
        hFunc(TEvEtcdKV::TEvPutResponse, Handle)

        hFunc(TEvEtcdKV::TEvDeleteRangeRequest, Handle)
        hFunc(TEvEtcdKV::TEvDeleteRangeResponse, Handle)

        hFunc(TEvEtcdKV::TEvTxnRequest, Handle)
        hFunc(TEvEtcdKV::TEvTxnResponse, Handle)

        hFunc(TEvEtcdKV::TEvCompactionRequest, Handle)
        hFunc(TEvEtcdKV::TEvCompactionResponse, Handle)
    )

    void Handle(TEvEtcdRevision::TEvCreateTableRequest::TPtr& ev) {
        std::cerr << "TEvEtcdRevision::TEvCreateTableRequest\n";
        Y_UNUSED(ev);
    }

    void Handle(TEvEtcdRevision::TEvRevisionResponse::TPtr& ev) {
        std::cerr << "TEvEtcdRevision::TEvRevisionResponse\n";
        Y_UNUSED(ev);
    }

    void Handle(TEvEtcdKV::TEvCreateTableRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvCreateTableRequest\n";
        Y_UNUSED(ev);
    }

    void Handle(TEvEtcdKV::TEvCreateTableResponse::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvCreateTableResponse\n";
        Y_UNUSED(ev);
    }

    void Handle(TEvEtcdKV::TEvRangeRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvRangeRequest\n";
        this->Send(ev->Sender, new TEvEtcdKV::TEvRangeResponse({}, {}, {}, {}), {}, {});
    }

    void Handle(TEvEtcdKV::TEvRangeResponse::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvRangeResponse\n";
        Y_UNUSED(ev);
    }

    void Handle(TEvEtcdKV::TEvPutRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvPutRequest\n";
        this->Send(ev->Sender, new TEvEtcdKV::TEvPutResponse({}, {}, {}, {}), {}, {});
    }

    void Handle(TEvEtcdKV::TEvPutResponse::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvPutResponse\n";
        Y_UNUSED(ev);
    }

    void Handle(TEvEtcdKV::TEvDeleteRangeRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvDeleteRangeRequest\n";
        this->Send(ev->Sender, new TEvEtcdKV::TEvDeleteRangeResponse({}, {}, {}, {}), {}, {});
    }

    void Handle(TEvEtcdKV::TEvDeleteRangeResponse::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvDeleteRangeResponse\n";
        Y_UNUSED(ev);
    }

    void Handle(TEvEtcdKV::TEvTxnRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvTxnRequest\n";
        this->Send(ev->Sender, new TEvEtcdKV::TEvTxnResponse({}, {}, {}, {}), {}, {});
    }

    void Handle(TEvEtcdKV::TEvTxnResponse::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvTxnResponse\n";
        Y_UNUSED(ev);
    }

    void Handle(TEvEtcdKV::TEvCompactionRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvCompactionRequest\n";
        this->Send(ev->Sender, new TEvEtcdKV::TEvCompactionResponse({}, {}, {}, {}), {}, {});
    }

    void Handle(TEvEtcdKV::TEvCompactionResponse::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvCompactionResponse\n";
        Y_UNUSED(ev);
    }

private:
    using TRequests = std::variant<
        TEvEtcdRevision::TEvCreateTableRequest::TPtr,
        TEvEtcdKV::TEvCreateTableRequest::TPtr,
        TEvEtcdKV::TEvRangeRequest::TPtr,
        TEvEtcdKV::TEvPutRequest::TPtr,
        TEvEtcdKV::TEvDeleteRangeRequest::TPtr,
        TEvEtcdKV::TEvTxnRequest::TPtr,
        TEvEtcdKV::TEvCompactionRequest::TPtr>;
};

NActors::IActor* CreateEtcdService() {
    return new TEtcdService();
}

} // namespace NYdb::NEtcd
