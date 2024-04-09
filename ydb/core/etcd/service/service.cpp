#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NYdb::NEtcd {

class TEtcdService : public NActors::TActorBootstrapped<TEtcdService> {
public:
    TEtcdService() = default;

    void Bootstrap() {}
};

NActors::IActor* CreateEtcdService() {
    return new TEtcdService();
}

} // namespace NYdb::NEtcd
