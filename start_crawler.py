import get_tenant_id
import get_tenant_info
import get_tenant_food


if __name__ == '__main__':

    # get_tenant_info.crawler()
    # get_tenant_food.crawler()

    get_tenant_id.process_jsn()
    get_tenant_info.process_jsn()
    get_tenant_food.process_jsn()
