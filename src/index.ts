import { RPC, RPCType, checkTopic } from 'castle-rpc'
import { base_covert } from 'castle-covert';
import { EventEmitter } from 'eventemitter3';
const max = 218340105584896;
export enum ServerEvent {
    LOGIN = 'LOGIN',
    SUBSCRIBE = 'SUBSCRIBE',
    UNSUBSCRIBE = 'UNSUBSCRIBE',
    PUBLISH = 'PUBLISH',
    PUSH = 'PUSH',
    MESSAGE = 'MESSAGE',
    CLOSED = 'CLOSED',
    REGIST = 'REGIST',
    UNREGIST = 'UNREGIST',
    REQUEST = 'REQUEST'
}
export class RPCServer extends EventEmitter {
    protected ClientAddress: number = 0
    protected clients: {
        [index: string]: {
            options: any,
            services: string[],
            subscribes: string[]
        }
    } = {}
    protected services: {
        [index: string]: {
            [index: string]: any
        }
    } = {}
    protected debug: boolean = false;
    constructor(options: { debug?: boolean }) {
        super()
        if (options) {
            if (options.debug) this.debug = options.debug
        }
    }
    async controller(path: string, data: any, rpc: RPC, options: any) {
        return true;
    }
    /**
     * 获取所有终端
     */
    getClients() {
        return Object.keys(this.clients)
    }
    /**
     * 获取指定客户端
     * @param ID 
     */
    getClient(ID: string) {
        return this.clients[ID]
    }
    /**
     * 获取所有服务
     */
    getServices() {
        return Object.keys(this.services)
    }
    /**
     * 查询具有某个服务的在线设备
     * @param ServiceName 
     */
    getServicesClients(ServiceName: string) {
        return this.services[ServiceName] ? Object.keys(this.services[ServiceName]) : []
    }
    protected _subscribes: { [index: string]: string[] } = {}
    /**
     * 服务端订阅
     * @param topic 
     * @param cb 
     * @param index 
     */
    async subscribe(topic: string, cb: Function, index: number | string = 0) {
        try {
            topic = checkTopic(topic);
            this._subscribes[topic][index] = cb
        } catch (error) {
            return false;
        }
    }
    /**
     * 处理服务端的发布请求
     * @param topic 
     * @param data 
     */
    async publish(topic: string, data: any) {
        Object.keys(this._subscribes).forEach((topic: string) => {
            if (new RegExp(topic).test(topic)) {
                Object.keys(this._subscribes[topic]).forEach((id: string) => {
                    this._subscribes[topic][id](data, '', topic)
                })
            }
        })
        let rpc = new RPC()
        rpc.From = ''
        rpc.Type = RPCType.Pub
        rpc.Data = data;
        rpc.NeedReply = false;
        let pubs = [];
        Object.keys(this._subscribes).forEach((t: string) => {
            if (new RegExp(t).test(topic)) {
                this._subscribes[t].forEach((id: string) => {
                    rpc.To = id;
                    rpc.Path = topic
                    try {
                        this.sendTo(id, rpc.encode())
                        pubs.push(id)
                        // console.log(`To:${rpc.To},ID:${rpc.ID},Data:${rpc.Data}`)
                    } catch (error) {

                    }
                })
            }
        })
        return pubs;
    }
    /**
     * 服务端取消订阅
     * @param topic 
     * @param index 
     */
    async unsubscribe(topic: string, index: number | string = 0) {
        try {
            topic = checkTopic(topic);
            if (this._subscribes[topic][index])
                delete this._subscribes[topic][index]
        } catch (error) {
            return false;
        }
    }
    /**
     * 处理来自客户端的发布请求
     * @param rpc 
     * @param ctx 
     */
    async handlePublish(rpc: RPC, ctx?: any) {
        //发布
        let pubs: string[] = [];
        // console.log(`From:${rpc.From},ID:${rpc.ID},Data:${rpc.Data}`)
        Object.keys(this._subscribes).forEach((topic: string) => {
            if (new RegExp(topic).test(rpc.Path)) {
                Object.keys(this._subscribes[topic]).forEach((id: string) => {
                    this._subscribes[topic][id](rpc.Data, rpc.From, rpc.Path)
                })
            }
        })
        Object.keys(this._subscribes).forEach((topic: string) => {
            if (new RegExp(topic).test(rpc.Path)) {
                this._subscribes[topic].forEach((id: string) => {
                    rpc.To = id;
                    try {
                        this.sendTo(id, rpc.encode(), ctx)
                        pubs.push(id)
                        // console.log(`To:${rpc.To},ID:${rpc.ID},Data:${rpc.Data}`)
                    } catch (error) {

                    }
                })
            }
        })
        rpc.To = rpc.From
        rpc.Data = pubs.length > 100 ? pubs.length : pubs;
        rpc.Type = RPCType.Response
        this.send(rpc, ctx)
    }
    /**
     * 登陆处理逻辑
     * @param rpc 
     * @param ctx 
     */
    async handleLogin(rpc: RPC, ctx: any) {
        rpc.Data = true
        rpc.Type = RPCType.Response
        if (this.clients[rpc.From]) {
            rpc.Status = false;
            rpc.Data = this.genClientAddress()
        } else {
            if (ctx.ID) {
                this.close(ctx)
            }
            ctx.ID = rpc.From;
            this.clients[rpc.From] = {
                options: ctx,
                services: [],
                subscribes: []
            };
        }
        rpc.To = rpc.From
        rpc.From = ''
        this.send(rpc, ctx)
    }
    /**
     * 发送
     * @param content 
     * @param options 
     */
    async send(content: RPC, options: any) {
        throw ServerError.UNKONW_SEND
    }
    /**
     * 发送
     * @param ID 
     * @param content 
     * @param options 
     */
    async sendTo(ID: string, content: string | Buffer, options?: any) {
        throw ServerError.UNKONW_SEND
    }
    /**
     * 消息
     * @param data 
     * @param options 
     */
    async message(data: any, options: {
        ID: string,
        [index: string]: any
    }) {
        let rpc: RPC;
        if ('string' == typeof data) {
            rpc = JSON.parse(data)
        } else if (data instanceof Buffer) {
            rpc = RPC.decode(data)
        } else if (data instanceof RPC) {
            rpc = data;
        } else {
            throw ServerError.UNKNOW_DATA
        }
        try {
            this.emit(ServerEvent.MESSAGE, rpc)
            switch (rpc.Type) {
                case RPCType.Request:
                    try {
                        this.emit(ServerEvent.REQUEST, rpc)
                        rpc.Data = await this.controller(rpc.Path, rpc.Data, rpc, options)
                    } catch (e) {
                        rpc.Data = { m: e.message }
                        if (this.debug) {
                            rpc.Data['e'] = e.stack
                        }
                    } finally {
                        if (rpc.NeedReply) {
                            rpc.Type = RPCType.Response
                            rpc.To = rpc.From;
                            rpc.From = ''
                            rpc.NeedReply = false;
                            this.send(rpc, options)
                        }
                    }
                    break;
                case RPCType.Proxy:
                    break;
                case RPCType.Response:
                    if (rpc.Status)
                        this.resolve(rpc.ID, rpc.Data)
                    else
                        this.reject(rpc.ID, rpc.Data)
                    break;
                case RPCType.Login:
                    this.handleLogin(rpc, options)
                    break;
                case RPCType.Regist:
                    if (!this.services[rpc.Path]) { this.services[rpc.Path] = {} }
                    if (rpc.Data) {
                        //注册
                        this.services[rpc.Path][options.ID] = options
                        this.clients[rpc.From].services.push(rpc.Path)
                        this.emit(ServerEvent.REGIST, rpc)
                    } else {
                        //注销                        
                        if (this.services[rpc.Path][options.ID])
                            delete this.services[rpc.Path][options.ID]
                        let i = this.clients[rpc.ID].services.indexOf(rpc.Path)
                        if (i > -1) { this.clients[rpc.ID].services.splice(i, 1) }
                        this.emit(ServerEvent.UNREGIST, rpc)
                    }
                    rpc.To = rpc.From
                    rpc.From = ''
                    rpc.Type = RPCType.Response
                    this.send(rpc, options)
                    break;
                case RPCType.Pub:
                    this.handlePublish(rpc, options)
                    break;
                case RPCType.Sub:
                    //订阅
                    try {
                        if ('string' == typeof rpc.Data) {
                            let topic = checkTopic(rpc.Data)
                            this.handleSubscribe(rpc.From, topic)
                        } else if (rpc.Data instanceof Array) {
                            rpc.Data.forEach((topic: string) => {
                                topic = checkTopic(topic)
                                this.handleSubscribe(rpc.From, topic)
                            })
                        } else {
                            rpc.Status = false;
                            rpc.Data = 'ErrorTopic'
                        }
                    } catch (error) {
                        rpc.Status = false;
                        rpc.Data = 'ErrorTopic'
                    } finally {
                        rpc.To = rpc.From
                        rpc.Type = RPCType.Response
                        this.send(rpc, options)
                    }

                    break;
                case RPCType.UnSub:
                    //取消订阅
                    try {
                        if ('string' == typeof rpc.Data) {
                            this.handleUnSubscribe(rpc.From, rpc.Data)
                        } else if (rpc.Data instanceof Array) {
                            rpc.Data.forEach((topic: string) => {
                                this.handleUnSubscribe(rpc.From, topic)
                            })
                        } else {
                            rpc.Status = false;
                            rpc.Data = 'ErrorTopic'
                        }
                    } catch (error) {
                        rpc.Status = false;
                        rpc.Data = 'ErrorTopic'
                    } finally {
                        if (rpc.Status) { rpc.Data = '' }
                        rpc.Type = RPCType.Response
                        this.send(rpc, options)
                    }
                    break;

            }
        } catch (error) {
            if (rpc.NeedReply) {
                rpc.Status = false;
                rpc.Data = error.message
                rpc.To = rpc.From;
                rpc.From = ''
                this.send(rpc, options)
            }
        }
    }
    protected handleSubscribe(ID: string, topic: string) {
        this.emit(ServerEvent.SUBSCRIBE, { ID, Topic: topic })
        if (!this._subscribes[topic]) { this._subscribes[topic] = [] }
        if (this._subscribes[topic].indexOf(ID) == -1) {
            this._subscribes[topic].push(ID)
            this.clients[ID].subscribes.push(topic)
        }
    }
    protected handleUnSubscribe(ID: string, topic: string) {
        topic = checkTopic(topic)
        this.emit(ServerEvent.UNSUBSCRIBE, { ID, Topic: topic })
        if (!this._subscribes[topic]) { this._subscribes[topic] = [] }
        let i = this._subscribes[topic].indexOf(ID)
        if (i > -1) { this._subscribes[topic].splice(i, 1) }
    }
    protected genClientAddress() {
        while (true) {
            if (this.clients[base_covert(10, 62, this.ClientAddress)]) {
                this.ClientAddress++
                if (this.ClientAddress > max) { this.ClientAddress = 0 }
            } else { return base_covert(10, 62, this.ClientAddress) }
        }
    }
    async push(to: string, path: string, data: any) {
        if (this.clients[to]) {
            let rpc = new RPC()
            rpc.Type = RPCType.Push;
            rpc.To = to;
            rpc.ID = 0;
            rpc.NeedReply = false;
            rpc.Path = path
            rpc.Data = data
            this.sendTo(to, rpc.encode(), this.clients[to].options)
        } else {
            throw ServerError.NOT_ONLINE
        }
    }
    async close(ctx) {
        if (ctx.ID && this.clients[ctx.ID]) {
            this.clients[ctx.ID].services.forEach((e: string) => {
                if (this.services[e][ctx.ID]) {
                    delete this.services[e][ctx.ID]
                }
            })
            this.clients[ctx.ID].subscribes.forEach((e: string) => {
                let i = this._subscribes[e].indexOf(ctx.ID);
                if (i > -1) {
                    this._subscribes[e].splice(i, 1)
                }
            })
            delete this.clients[ctx.ID]
            this.emit(ServerEvent.CLOSED, ctx)
        }
    }
    async request(to: string, path: string, data: any, options: { NeedReply?: Boolean, Timeout?: number } = {}) {
        if (!this.clients[to]) {
            throw ServerError.NOT_ONLINE
        }
        let r = new RPC()
        r.Path = path;
        r.Data = data;
        r.From = '';
        r.To = to;
        r.Type = RPCType.Request
        r.Time = Date.now()
        if (options.Timeout && options.Timeout > 0) {
            r.Timeout = Number(options.Timeout)
            setTimeout(() => {
                this.reject(r.ID, ServerError.TIMEOUT)
            }, options.Timeout)
        }
        this.sendTo(to, r.encode(), this.clients[to].options)
        if (options.NeedReply !== false) {
            r.NeedReply = true;
            return new Promise((resolve, reject) => {
                this._promise[r.ID] = { resolve, reject }
            })
        }
        return true;
    }
    _promise = {};
    /**
     * 成功处理
     * @param ID 请求编号
     * @param data 响应数据
     */
    protected resolve(ID: number, data: any) {
        if (this._promise[ID]) {
            this._promise[ID].resolve(data)
            delete this._promise[ID]
        }
    }
    /**
     * 失败处理
     * @param ID 请求编号
     * @param data 响应数据
     */
    protected reject(ID: number, data: any) {
        if (this._promise[ID]) {
            this._promise[ID].reject(data)
            delete this._promise[ID]
        }
    }
}
export enum ServerError {
    UNKNOW_DATA,
    UNKONW_SEND,
    NOT_ONLINE,
    TIMEOUT,
}